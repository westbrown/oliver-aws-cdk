import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="oliver-db",
    table_name="oliver_apache_bucket",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("host", "string", "host", "string"),
        ("ident", "string", "ident", "string"),
        ("authuser", "string", "authuser", "string"),
        ("datetime", "string", "datetime", "string"),
        ("request", "string", "request", "string"),
        ("response", "string", "response", "string"),
        ("bytes", "string", "bytes", "string"),
        ("partition_0", "string", "partition_0", "string"),
        ("partition_1", "string", "partition_1", "string"),
        ("partition_2", "string", "partition_2", "string"),
        ("partition_3", "string", "partition_3", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Aggregate
Aggregate_node1659854971710 = sparkAggregate(
    glueContext,
    parentFrame=ApplyMapping_node2,
    groups=["partition_0", "partition_1", "partition_2", "partition_3", "response"],
    aggs=[["host", "count"]],
    transformation_ctx="Aggregate_node1659854971710",
)

# Script generated for node Amazon S3
AmazonS3_node1659855762148 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1659854971710,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://oliver-apache-agg-bucket/",
        "partitionKeys": ["partition_0", "partition_1", "partition_2"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1659855762148",
)

job.commit()
