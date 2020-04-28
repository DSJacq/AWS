#!/bin/bash Python 3

# import libraries
import boto3
from boto3.session import Session

# define access credentials to S3
aws_access_key_id     ="XXXXXXXXXXXXXXXXXXXXX"
aws_secret_access_key ="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# start session
session = Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3 = session.resource("s3")

# define bucket
bucket = "BUCKET"
bucket_conn = s3.Bucket(bucket)

# folder path to delete
# path/*.txt
def delete_file(file_name):
    for s3_file in bucket_conn.objects.all():
        subdir = s3_file.key.split('/')
        if subdir[0] == "FOLDER" and subdir[1] == file_name in s3_file.key:
            try:
                print(s3_file.key)
                file = s3_file.key
                client = boto3.client('s3')
                client.delete_object(Bucket="BUCKET", Key=file)
                # print("Files deleted: " + s3_file.key)
            except: 
                print("No files to delete. Check if the table was uploaded before starting ETL procedure.")

# crete folder to receive the file next day
def create_folder(file_name, folder_name):
    client = boto3.client("s3")
    client.put_object(Bucket=bucket, Body="", Key="KEY/" + file_name + "/" + folder_name + "/")
    # print("Folder created successfully for " + file_name + ".") 


myList = ["Table1", "Table2", "Table3", "Table4", "Table5"]
        
for i in myList:
    
    # Set the function parameters
    file_name = i
    folder_name = "RECREATE FOLDER"
    
    # Apply the function delete_file
    delete_file(file_name)
    
    # Apply the function create_folder
    create_folder(file_name, folder_name)
        
print("Finished deleting files from folder A.")
