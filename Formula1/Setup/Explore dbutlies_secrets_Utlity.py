# Databricks notebook source
# MAGIC %md
# MAGIC #Exploring the dbutlis. Secrets.Utlity 

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

sharedkey= dbutils.secrets.get(scope="formula1-scope", key="formula1-SAStoken")
storagekey= dbutils.secrets.get(scope="formula1-scope", key="formula1-Accesskey")

# COMMAND ----------

storagekey

# COMMAND ----------


