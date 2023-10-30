# Databricks notebook source
# MAGIC %md
# MAGIC #Step 1 --> Read results data, develop schema before that
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

# COMMAND ----------

#Importing Sql functions to Databricks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


# COMMAND ----------


# Define the schema using StructType and StructField
results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# Now, you can use this schema to read or create DataFrames


# COMMAND ----------

results_df= (spark
             .read
             .format("json")
             .schema(results_schema)
             .load(f"{raw_folder_path}/results.json"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2 Transformations 
# MAGIC 1. Change column names 
# MAGIC 2. ingest data column 
# MAGIC 3. drop unwanted columns 
# MAGIC 4. Partation based on race id

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

renamed_df= (results_df.withColumnRenamed("resultId", "result_id")
             .withColumnRenamed("raceId", "race_id")
             .withColumnRenamed("driverId", "driver_id")
             .withColumnRenamed("constructorId", "constructor_id")
             .withColumnRenamed("positionText", "position_text")
             .withColumnRenamed("positionOrder", "position_order")
             .withColumnRenamed("fastestLap", "fastest_lap")
             .withColumnRenamed("fastestLapTime", "fastest_lap_time")
             .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
             .transform(add_ingestion_data)
             .withColumn("data_source",lit(v_data_source))
             )
    

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

results_final_df= renamed_df.drop(col("status_id"))
display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3--> Write to output to processed container in parquet format

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.format("parquet").saveAsTable("f1_processed.results"))

# COMMAND ----------

dbutils.notebook.exit("sucess")
