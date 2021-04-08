#!/bin/bash Spark2.4, Python 3



# import libraries of interest

import sys
import datetime

from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

# set context
run_date = datetime.datetime.now().strftime("%Y%m%d%H%M")
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# set custom logging on
logger = glueContext.get_logger()
...
# write into the log file with:
logger.info("write log here ... ")

# Load table ZMMTFL111 Full
load_table = spark.read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("sep", "|")
                .option("inferSchema", "true")
                .option("encoding", "ISO-8859-1")
                .load("s3://bucket/folder/")

# create an alias for each table
df = load_table.alias("df")

# set tables as distinct
df = df.distinct().cache()

# treat numeric columns 
for col_name in df.columns:
    # replace "," by "0,"
    df = df.withColumn(col_name,  f.when(f.col(col_name).startswith(","), regexp_replace(f.col(col_name), ",", "0.")).otherwise(f.col(col_name)))
    # replace "," by "."
    df = df.withColumn(col_name, regexp_replace(col_name, ",", "."))

df = df.distinct().cache()

