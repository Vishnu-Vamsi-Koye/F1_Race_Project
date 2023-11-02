# Databricks notebook source
# MAGIC %md
# MAGIC #Step 1 --> Read results data, develop schema before that
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

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
             .load(f"{raw_folder_path}/{v_file_date}/results.json"))

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
             .withColumn("file_date",lit(v_file_date))
             )
    

# COMMAND ----------

display(renamed_df)

# COMMAND ----------

results_final_df= renamed_df.drop(col("status_id"))
display(results_final_df)

# COMMAND ----------

results_deduped_df= results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3--> Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark.catalog.tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id= {race_id_list.race_id})")


# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# %sql
# SELECT race_id, driver_id, count(1) as races_count
# FROM f1_processed.results
# GROUP BY race_id, driver_id
# HAVING count(1)>1
# ORDER BY race_id DESC


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Method 2

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# results_final_df=results_final_df.select(['result_id',
#  'driver_id',
#  'constructor_id',
#  'number',
#  'grid',
#  'position',
#  'position_text',
#  'position_order',
#  'points',
#  'laps',
#  'time',
#  'milliseconds',
#  'fastest_lap',
#  'rank',
#  'fastest_lap_time',
#  'fastest_lap_speed',
#  'statusId',
#  'ingestion_date',
#  'data_source',
#  'file_date','race_id'])

# COMMAND ----------

# if(spark.catalog.tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------


# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

# from delta.tables import DeltaTable
# if spark.catalog.tableExists("f1_processed.results"):
#     deltaTable = DeltaTable.forPath(spark, f"{processed_folder_path}/results")
#     deltaTable.alias("tgt").merge(
#         results_final_df.alias("src"),
#         "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
#     ) \
#     .whenMatchedUpdateAll() \
#     .whenNotMatchedInsertAll() \
#     .execute()
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition= "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# overwrite_partation(results_final_df, 'f1_processed','results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,driver_id, count(1) as races_count
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id,driver_id
# MAGIC
# MAGIC ORDER BY race_id DESC
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("sucess")
