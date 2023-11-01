# Databricks notebook source
# MAGIC %md
# MAGIC #Step 1-- Read data file JSOn using spark data frame 

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

#Importing Sql functions to Databricks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType, DateType

# COMMAND ----------

pit_stops_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
                                     ])

# COMMAND ----------

pits_stop_df= (spark
             .read
             .format("json")
             .option("multiline", True)
             .schema(pit_stops_schema)
             .load(f"{raw_folder_path}/{v_file_date}/pit_stops.json"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2- Transformatins of Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

renamed_pits_stop_df= pits_stop_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .transform(add_ingestion_data).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

overwrite_partation(renamed_pits_stop_df, 'f1_processed','pit_stops', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) as races
# MAGIC  FROM f1_processed.pit_stops
# MAGIC  GROUP BY race_id
# MAGIC  ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("sucess")
