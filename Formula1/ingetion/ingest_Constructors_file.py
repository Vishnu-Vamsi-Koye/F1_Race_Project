# Databricks notebook source
# MAGIC %md 
# MAGIC #Step-> 1 -- Defining Schema using DDL Approch 

# COMMAND ----------

Con_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2-- Read the JSON file using Spark dataframe reader

# COMMAND ----------

Constructor_df= spark.read.json("abfss://raw@dbformuladl.dfs.core.windows.net/constructors.json", schema=Con_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 3--Transformations
# MAGIC 1. Drop Column of URL
# MAGIC 2. Rename the column names

# COMMAND ----------

con_df_droped= Constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

renamed_df= con_df_droped.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingestion_data", current_timestamp())


# COMMAND ----------

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4-- Write data into processed folder

# COMMAND ----------

renamed_df.write.mode("overwrite").parquet("abfss://processed@dbformuladl.dfs.core.windows.net/Constructors")
