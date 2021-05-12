#!/bin/bash Spark2.4, Python 3

import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameReader, DynamicFrameWriter, DynamicFrameCollection
from awsglue.job import Job

# create glue context from spark context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# ead parquet data from S3 using spark + glue context
df_raw = spark.read.parquet("s3://bucket/folder/folder/*")

# read parquet data from S3 using glueContext
df = df_raw.cache()

