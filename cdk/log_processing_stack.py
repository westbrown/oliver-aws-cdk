from imp import source_from_cache
from mimetypes import init
from os import environ
import os.path
from unicodedata import name
from aws_cdk.aws_s3_assets import Asset
import json
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
    aws_athena as athena,
    aws_kinesis as kinesis,
    aws_quicksight as quicksight,
    aws_kinesisfirehose as firehose,
    App, Stack
)
from constructs import Construct
dirname = os.path.dirname(__file__)


class LogProcessStack(Stack):

    def init_vpc(self):
        self.vpc = ec2.Vpc(self, "VPC", nat_gateways=0, cidr="10.0.0.0/16", vpc_name="log-vpc",
                           subnet_configuration=[ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC)])

    def init_ec2(self):
        amzn_linux = ec2.MachineImage.latest_amazon_linux(
            generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
            edition=ec2.AmazonLinuxEdition.STANDARD,
            virtualization=ec2.AmazonLinuxVirt.HVM,
            storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE
        )
        # instance role and ssm managed policy
        role = iam.Role(self, "InstanceSSM", assumed_by=iam.ServicePrincipal(
            "ec2.amazonaws.com"), role_name="log-ec2-role")
        for policy_item in ["CloudWatchFullAccess", "AmazonSSMManagedInstanceCore", "AmazonKinesisFirehoseFullAccess"]:
            role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(policy_item))

        security_group = ec2.SecurityGroup(
            self, "SecurityGroup", vpc=self.vpc, security_group_name="log-ec2-sg")
        security_group.add_ingress_rule(ec2.Peer.ipv4(
            "0.0.0.0/0"), ec2.Port.tcp(22), "Allow ssh")
        security_group.add_ingress_rule(ec2.Peer.ipv4(
            "0.0.0.0/0"), ec2.Port.tcp(80), "Allow http")

        instance = ec2.Instance(self, "Instance",
                                instance_type=ec2.InstanceType("t2.micro"),
                                machine_image=amzn_linux,
                                vpc=self.vpc,
                                role=role,
                                key_name="oliver",  # replace your own key-pem
                                instance_name="log-ec2-server",
                                security_group=security_group,
                                )

        asset = Asset(self, "Asset", path=os.path.join(
            dirname, "ec2_init_script.sh"))
        local_path = instance.user_data.add_s3_download_command(
            bucket=asset.bucket,
            bucket_key=asset.s3_object_key
        )

        instance.user_data.add_execute_file_command(
            file_path=local_path
        )
        asset.grant_read(instance.role)
        self.instance = instance

    def init_s3(self):
        self.bucket = s3.Bucket(
            self, 'Myfirstbucket', versioned=False, bucket_name="oliver-apache-bucket")
    
    def init_agg_s3(self):
        self.agg_bucket = s3.Bucket(
            self, "ResultBucket", bucket_name="oliver-apache-agg-bucket", versioned=False)

    def init_kinesis_firehose(self):
        role = iam.Role(self, "FireHoseRole",assumed_by=iam.ServicePrincipal(
            "firehose.amazonaws.com"), role_name="log-ingest-role")
        for policy_item in ["CloudWatchFullAccess", "AmazonKinesisFirehoseFullAccess"]:
            role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(policy_item))
        self.bucket.grant_read_write(role)

        firehose.CfnDeliveryStream(self, "MyCfnDeliveryStream", 
            delivery_stream_name = "web-log-ingestion-stream",
            delivery_stream_type = "DirectPut",
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn = self.bucket.bucket_arn,
                role_arn = role.role_arn
            )
        )

    def init_glue_crawler(self):
        role = iam.Role(self, "CrawlerRole", assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name = "log-glue-crawler-role")
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        self.bucket.grant_read_write(role)

        self.glue_role = role
        glue_db_name = "oliver-db"
        glue_database = glue.CfnDatabase(self, "MyGlueDb", 
            catalog_id= self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name = glue_db_name,
               # target_database=glue.CfnDatabase.DatabaseIdentifierProperty(
                #    catalog_id = self.account,
                #    #database_name=glue_db_name
                #)
            )
        )
        self.glue_database = glue_db_name

        print(glue_database.database_input.name)
        glue.CfnCrawler(self, "CfnCrawler",
            role = role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path = self.bucket.bucket_name,
                )]
            ),
            database_name=glue_db_name,
            name="web-raw-log-crawler",
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="Cron(*/5 * * * ? *)"
            ),
            table_prefix=""
        )
    
    def init_etl_job(self):
        asset = Asset(self, "CodeAsset", path=os.path.join(
            dirname, "etl_pyspark.py"))
        self.glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))
        etl_job = glue.CfnJob(self, "glueETLJob", 
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location="%s" % asset.s3_object_url
            ),
            role=self.glue_role.role_arn,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version = "3.0",
            max_retries=1,
            name="glue_etl_job",
            number_of_workers=5,
            timeout=120,
            worker_type="G.1X"
        )

        #add schedule trigger for etl_job
        glue.CfnTrigger(self, "MyCfnTrigger",
            actions=[glue.CfnTrigger.ActionProperty(
                job_name=etl_job.name,
                timeout=120
            )],
            type = "SCHEDULED",
        
            name="etl_daily_trigger",
            schedule="Cron(20 10 * * ? *)",
            start_on_creation=True,
        )
        

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # vpc
        self.init_vpc()
        # ec2
        self.init_ec2()
        # s3 create for storing apache-log
        self.init_s3()
        # kinesis firehose
        self.init_kinesis_firehose()

        # create glue_table
        self.init_glue_crawler()

        # aggregate response_cnt for apache-raw-log group by date, response_code
        # init agg result bucket
        self.init_agg_s3()
        # init glue_etl_spark_job to do aggregation and sink result to s3
        self.init_etl_job()




