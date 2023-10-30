# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure Data Lake using Access keys
# MAGIC 1. Set Spark config 
# MAGIC 2. List files from Demo Container
# MAGIC 3. List data in circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dbformuladl.dfs.core.windows.net",
    "JT9j877juknSciQ9DEJ7cbJ+RHgMinhwZGKn7uJEuEea4CFY0nf79WOce/12mZGI4Yszv+egrGWQ+AStjQGRUg==")

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbformuladl.dfs.core.windows.net"))

# COMMAND ----------

Df= spark.read.csv("abfss://demo@dbformuladl.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(Df)

# COMMAND ----------


