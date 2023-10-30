# Databricks notebook source
# MAGIC %md
# MAGIC #Step 1-- Read data file JSOn using spark data frame 

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

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
             .load(f"{raw_folder_path}/pit_stops.json"))

# COMMAND ----------

pits_stop_df

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2- Transformatins of Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

renamed_pits_stop_df= pits_stop_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .transform(add_ingestion_data).withColumn("data_source",lit(v_data_source))

# COMMAND ----------

renamed_pits_stop_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("sucess")
