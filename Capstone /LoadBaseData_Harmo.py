# Databricks notebook source
container_name = "basedata"
account_name = "capstonp"
storage_account_key = "+NbWQyuYaB/+tu53Ghq5qlrpqff+pL0Jdtrj7nD7KO99Vn/0Ro5wnjIBAVrxPzfuKH6MUq1IAxoo+AStm/3b9A=="

# dbutils.fs.mount(
# source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
# mount_point = "/mnt/basedata",
#  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
#   )

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
from io import BytesIO

# COMMAND ----------

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/d92d5094-56ee-43b1-afc1-d6d844d547d5_83d04ac6-cb74-4a96-a06a-e0d5442aa126_Telecom.zip"

local_zip_file_path = "/dbfs/mnt/basedata/data/data.zip"
local_unzip_dir = "/dbfs/mnt/basedata/data/unzipped_data"

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=capstonp;AccountKey=+NbWQyuYaB/+tu53Ghq5qlrpqff+pL0Jdtrj7nD7KO99Vn/0Ro5wnjIBAVrxPzfuKH6MUq1IAxoo+AStm/3b9A==;EndpointSuffix=core.windows.net"

folder_name = "unzipped"  

# COMMAND ----------

subprocess.run(["wget", "-O", local_zip_file_path, zip_file_url], check=True)
subprocess.run(["unzip", "-q", local_zip_file_path, "-d", local_unzip_dir], check=True)

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_files = os.listdir(local_unzip_dir)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    blob_path = os.path.join(folder_name, file_name)

    blob_client = container_client.get_blob_client(blob_path)

    with open(local_file_path, "rb") as data:

        blob_client.upload_blob(data)

 

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")

 
os.remove(local_zip_file_path)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    os.remove(local_file_path)

print("Cleaned up local files")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/basedata/unzipped/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/basedata/unzipped"))

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Billing_partition_1.csv"
billing_partition_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

billing_partition_df.printSchema()

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Customer_information.csv"
customer_information_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

# Rename the column 'system status' to 'system_status'
customer_information_df = customer_information_df.withColumnRenamed("system status", "system_status")

# Save the updated DataFrame as a CSV file in the specified path
output_path = "dbfs:/mnt/basedata/unzipped/Customer_information_updated.csv"
customer_information_df.write.mode("overwrite").csv(output_path, header=True)

# COMMAND ----------

customer_information_df.printSchema()

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Customer_rating.csv"
customer_rating_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

customer_rating_df.printSchema()

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Plans.csv"
plan_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

renamed_columns = {
    "Voice Service": "Voice_Service",
    "Mobile Data": "Mobile_Data",
    "Spam Detection": "Spam_Detection",
    "Fraud Prevention": "Fraud_Prevention"
}

for old_name, new_name in renamed_columns.items():
    plan_df = plan_df.withColumnRenamed(old_name, new_name)

# Save the modified DataFrame as a CSV file
output_path = "dbfs:/mnt/basedata/unzipped/Plans_updated.csv"
plan_df.write.mode("overwrite").csv(output_path, header=True)

# COMMAND ----------

plan_df.printSchema()

# COMMAND ----------

device_information_df = spark.read.json("dbfs:/mnt/basedata/unzipped/Device_Information.json")
<<<<<<< Updated upstream:LoadBaseData.py
=======

# COMMAND ----------

device_information_df.printSchema()

# COMMAND ----------


>>>>>>> Stashed changes:Capstone /LoadBaseData_Harmo.py
