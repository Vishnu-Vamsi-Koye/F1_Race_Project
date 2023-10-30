# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Data Lake using Access keys
# MAGIC 1. Set Spark config 
# MAGIC 2. List files from Demo Container
# MAGIC 3. List data in circuits.csv file

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope", key="formula1-Accesskey")

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@dbformuladl.dfs.core.windows.net"))

# COMMAND ----------

Df= spark.read.csv("abfss://raw@dbformuladl.dfs.core.windows.net/races.csv")

# COMMAND ----------

display(Df)

# COMMAND ----------


