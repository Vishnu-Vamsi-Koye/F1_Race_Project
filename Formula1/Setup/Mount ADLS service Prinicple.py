# Databricks notebook source
# MAGIC %md
# MAGIC #Mount ADLS service Prinicple
# MAGIC 1. Get Client id. tentent_id, and Client_seceret
# MAGIC
# MAGIC
# MAGIC Note: Noot Praticed

# COMMAND ----------

# MAGIC %fs ls FileStore

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbformuladl.dfs.core.windows.net"))

# COMMAND ----------

sharedkey= dbutils.secrets.get(scope="formula1-scope", key="formula1-SAStoken")
storagekey= dbutils.secrets.get(scope="formula1-scope", key="formula1-Accesskey")

# COMMAND ----------

configs = {
        "spark.hadoop.fs.azure.account.auth.type.dbformuladl.dfs.core.windows.net":sharedkey,
        "spark.hadoop.fs.azure.account.auth.sharedkey.dbformuladl.dfs.core.windows.net":storagekey
    }

# COMMAND ----------

configs


# COMMAND ----------

# Use the data from the mounted location
df = spark.read.parquet("/mnt/<mount_name>/<path_to_data>")
