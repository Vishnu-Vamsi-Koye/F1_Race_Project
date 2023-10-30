# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Data Lake using Service principle
# MAGIC 1. Register AD application/ Service Principle
# MAGIC 2. Generate Secrate password for the application
# MAGIC 3. Set spark config
# MAGIC 4. Assign role "Storage data Blob Contributer" to Data Lake

# COMMAND ----------

sas_key=dbutils.secrets.get(scope="formula1-scope", key="formula1-Accesskey")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbformuladl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dbformuladl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dbformuladl.dfs.core.windows.net", sas_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbformuladl.dfs.core.windows.net"))

# COMMAND ----------

Df= spark.read.csv("abfss://demo@dbformuladl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(Df)

# COMMAND ----------


