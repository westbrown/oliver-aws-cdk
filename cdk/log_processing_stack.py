from mimetypes import init
from os import environ
import os.path
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
            self, 'myfirstbucket', versioned=False, bucket_name="oliver-apache-bucket")

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        
        # vpc
        self.init_vpc()
        # ec2
        self.init_ec2()
        # s3 create for storing apache-log
        self.init_s3()
