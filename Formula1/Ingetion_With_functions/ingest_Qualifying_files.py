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

qualifying_schema= StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df= (spark
             .read
             .format("json")
             .option("multiline", True)
             .schema(qualifying_schema)
             .load(f"{raw_folder_path}/qualifying/"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2- Transformatins of Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

renamed_qualify_df= qualifying_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .transform(add_ingestion_data).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(renamed_qualify_df)

# COMMAND ----------

renamed_qualify_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("sucess")
