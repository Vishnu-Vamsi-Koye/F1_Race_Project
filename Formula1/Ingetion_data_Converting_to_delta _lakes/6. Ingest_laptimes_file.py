# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC #Step --> 1 : Read multiple csv data files into spark  

# COMMAND ----------

#Importing Sql functions to Databricks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType, DateType

# COMMAND ----------

lap_times_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
                                     ])

# COMMAND ----------

lap_times_df= (spark.read
               .format("csv")
               .schema(lap_times_schema)
               .load(f"{raw_folder_path}/{v_file_date}/lap_times/")
               )

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 2--> Transformations of Data 
# MAGIC 1. Renaming the columns 
# MAGIC 2. Adding the ingestion date column with current time stamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

renamed_laptime_df= lap_times_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .transform(add_ingestion_data).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3-- Write data into ADLS processed container  

# COMMAND ----------

# overwrite_partation(renamed_laptime_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition= "tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(renamed_laptime_df,'f1_processed','lap_times',processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT race_id, count(1) as races
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("sucess")

# COMMAND ----------


