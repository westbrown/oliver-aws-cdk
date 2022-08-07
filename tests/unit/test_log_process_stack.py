import aws_cdk as core
import aws_cdk.assertions as assertions
from cdk.log_processing_stack import LogProcessStack

def test_vpc_create():
    app = core.App()
    stack = LogProcessStack(app, "log-stack")
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties("AWS::EC2::VPC", {
        "CidrBlock": "10.0.0.0/16"
    })

def test_ec2_create():
    app = core.App()
    stack = LogProcessStack(app, "log-stack")
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties("AWS::EC2::SecurityGroup", {
        "GroupName": "log-ec2-sg"
    })

def test_s3_create():
    app = core.App()
    stack = LogProcessStack(app, "log-stack")
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName":"oliver-apache-bucket"
    })
    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName":"oliver-apache-agg-bucket"
    })

def test_kinesis():
    app = core.App()
    stack = LogProcessStack(app, "log-stack")
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties("AWS::IAM::Role", {
        "RoleName" : "log-ingest-role"
    })
    template.has_resource_properties("AWS::KinesisFirehose::DeliveryStream", {
        "DeliveryStreamName": "web-log-ingestion-stream"
    })

def test_glue():
    app = core.App()
    stack = LogProcessStack(app, "log-stack")
    template = assertions.Template.from_stack(stack)
    template.has_resource_properties("AWS::Glue::Database", {
        "DatabaseInput" : {
            "Name": "oliver-db"
        }
    })
    template.has_resource_properties("AWS::Glue::Crawler", {
        "DatabaseName": "oliver-db",
        "Name": "web-raw-log-crawler",
    })
    template.has_resource_properties("AWS::Glue::Job", {
        "Command": {
            "Name": "glueetl"
        }
    })
    template.has_resource_properties("AWS::Glue::Trigger", {
        "Type": "SCHEDULED"
    })
    template.has_resource_properties("AWS::Glue::Crawler", {
        "Name": "web-agg-log-crawler"
    })
