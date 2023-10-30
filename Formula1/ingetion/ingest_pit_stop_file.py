# Databricks notebook source
# MAGIC %md
# MAGIC #Step 1-- Read data file JSOn using spark data frame 

# COMMAND ----------

# MAGIC %fs ls abfss://processed@dbformuladl.dfs.core.windows.net/

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
             .load("abfss://raw@dbformuladl.dfs.core.windows.net/pit_stops.json"))

# COMMAND ----------

pits_stop_df

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2- Transformatins of Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

renamed_pits_stop_df= pits_stop_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

renamed_pits_stop_df.write.mode("overwrite").parquet("abfss://processed@dbformuladl.dfs.core.windows.net/pit_stops")
