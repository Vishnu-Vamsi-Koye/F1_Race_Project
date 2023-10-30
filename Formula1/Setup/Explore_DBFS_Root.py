# Databricks notebook source
# MAGIC %md 
# MAGIC #Explore DBFS Root
# MAGIC 1. List all folders in DBFS root
# MAGIC 2. Intract with DBFS filr Browser
# MAGIC 3. Upload file to DBFS root

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/airlines/part-00001

# COMMAND ----------

display(dbutils.fs.ls ('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------


