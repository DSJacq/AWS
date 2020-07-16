#!/bin/bash Spark2.4, Python 3

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameReader, DynamicFrameWriter, DynamicFrameCollection
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# create glue context from spark context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# set custom logging on
logger = glueContext.get_logger()
...

# write into the log file with:
logger.info("Type log info here ...")

# load table that is on S3 bucket
load_table = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("sep", "|")
            .load("s3://bucket/path/folder/")

logger.info("load table ...")

# creat an alias for the table
df = load_table.alias("df")

# set table as distinct
df = df.distinct().cache()

