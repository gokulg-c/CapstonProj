# Databricks notebook source
dbutils.fs.unmount("/mnt/basedata")

# COMMAND ----------

container_name = "basedata"
account_name = "capstonp"
storage_account_key = "+NbWQyuYaB/+tu53Ghq5qlrpqff+pL0Jdtrj7nD7KO99Vn/0Ro5wnjIBAVrxPzfuKH6MUq1IAxoo+AStm/3b9A=="
dbutils.fs.mount(
    source="wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
    mount_point="/mnt/basedata",
    extra_configs={"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/basedata/unzipped/"))

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Billing_partition_1.csv"
billing_partition_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Customer_information.csv"
customer_information_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Customer_rating.csv"
customer_rating_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

path = "dbfs:/mnt/basedata/unzipped/Plans.csv"
plan_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

device_information_df = spark.read.json("dbfs:/mnt/basedata/unzipped/Device_Information.json")
