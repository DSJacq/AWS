# https://docs.aws.amazon.com/en_en/glue/latest/dg/aws-glue-programming-etl-connect-samples.html

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

output_path = "s3://some_bucket/output/" + str(time.time()) + "/"
mongo_uri = "mongodb://<mongo-instanced-ip-address>:27017"
mongo_ssl_uri = "mongodb://<mongo-instanced-ip-address>:27017"
documentdb_uri = "mongodb://<mongo-instanced-ip-address>:27017"
write_uri = "mongodb://<mongo-instanced-ip-address>:27017"
documentdb_write_uri = "mongodb://<mongo-instanced-ip-address>:27017"

read_docdb_options = {
    "uri": documentdb_uri,
    "database": "test",
    "collection": "coll",
    "username": "username",
    "password": "1234567890",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"
}

read_mongo_options = {
    "uri": mongo_uri,
    "database": "test",
    "collection": "coll",
    "username": "username",
    "password": "pwd",
    "partitioner": "MongoSamplePartitioner",
    "partitionerOptions.partitionSizeMB": "10",
    "partitionerOptions.partitionKey": "_id"}

ssl_mongo_options = {
    "uri": mongo_ssl_uri,
    "database": "test",
    "collection": "coll",
    "ssl": "true",
    "ssl.domain_match": "false"
}

write_mongo_options = {
    "uri": write_uri,
    "database": "test",
    "collection": "coll",
    "username": "username",
    "password": "pwd"
}
write_documentdb_options = {
    "uri": documentdb_write_uri,
    "database": "test",
    "collection": "coll",
    "username": "username",
    "password": "pwd"
}

# Get DynamicFrame from MongoDB and DocumentDB
dynamic_frame = glueContext.create_dynamic_frame.from_options(connection_type="mongodb",
                                                              connection_options=read_mongo_options)
dynamic_frame2 = glueContext.create_dynamic_frame.from_options(connection_type="documentdb",
                                                               connection_options=read_docdb_options)

# Write DynamicFrame to MongoDB and DocumentDB
glueContext.write_dynamic_frame.from_options(dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
glueContext.write_dynamic_frame.from_options(dynamic_frame2, connection_type="documentdb",
                                             connection_options=write_documentdb_options)

job.commit()
