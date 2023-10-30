# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step-> 1 -- Defining Schema using DDL Approch 

# COMMAND ----------

Con_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2-- Read the JSON file using Spark dataframe reader

# COMMAND ----------

Constructor_df= spark.read.json(f"{raw_folder_path}/constructors.json", schema=Con_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 3--Transformations
# MAGIC 1. Drop Column of URL
# MAGIC 2. Rename the column names

# COMMAND ----------

con_df_droped= Constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df= con_df_droped.withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
    .transform(add_ingestion_data).withColumn("data_source", lit(v_data_source))


# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4-- Write data into processed folder

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("sucess")
