import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args["JOB_NAME"], args)


# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://stored-data-bucket1"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)
df1 = S3bucket_node1.toDF()
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col
df2 = df1.withColumn("timestamp",to_timestamp(col("timestamp"))).withColumn("year", date_format(col("timestamp"), "y")).withColumn("month", date_format(col("timestamp"), "M")).withColumn("day", date_format(col("timestamp"), "d")).withColumn("hour", date_format(col("timestamp"), "H"))
gluedf = DynamicFrame.fromDF( df2, glueContext, "gluedf")

"""
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=gluedf,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://stored-data-bucket1", "partitionKeys": ["year", "month", "day", "hour"]},
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)
"""

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://stored-data-bucket2",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["year", "month", "day", "hour"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(catalogDatabase="test-db", catalogTableName="test-table")
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(gluedf)
job.commit()
