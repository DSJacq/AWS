#!/bin/bash Spark 2.4, Python3

# import libraries of interest
import sys
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# create glue context from spark context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# set custom logging on
logger = glueContext.get_logger()
...
# write into the log file with:
logger.info("Write log here ...")

# read parquet
df = spark.read.parquet("s3://path/folder/folder/*")
df = df.cache()

# Upload to Redshift
# create dynamic drame
glue_dynamic_frame_union = DynamicFrame.fromDF(df, glueContext, "df")

# write table on redshift
schema_table = "schema.table"
database = "database
catalog_connection = "Redshift-X"
redshift_tmp_dir = "s3://bucket/folder/redshift_tmpdir_folder"
datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = glue_dynamic_frame_union, 
            catalog_connection = catalog_connection, 
            connection_options = {"dbtable":  schema_table, "database": database}, 
            redshift_tmp_dir=redshift_tmp_dir, 
            transformation_ctx = "datasink1")
logger.info("Upload on Redshift finished sucessfully...")
