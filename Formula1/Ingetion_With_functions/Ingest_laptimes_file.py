# Databricks notebook source
dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

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
               .load(f"{raw_folder_path}/lap_times/")
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
    .transform(add_ingestion_data).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3-- Write data into ADLS processed container  

# COMMAND ----------

renamed_laptime_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("sucess")
