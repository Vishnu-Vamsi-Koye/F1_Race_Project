# Databricks notebook source
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
               .load("abfss://raw@dbformuladl.dfs.core.windows.net/lap_times/")
               )

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 2--> Transformations of Data 
# MAGIC 1. Renaming the columns 
# MAGIC 2. Adding the ingestion date column with current time stamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

renamed_laptime_df= lap_times_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3-- Write data into ADLS processed container  

# COMMAND ----------

renamed_laptime_df.write.mode("overwrite").parquet("abfss://processed@dbformuladl.dfs.core.windows.net/lap_times")

# COMMAND ----------


