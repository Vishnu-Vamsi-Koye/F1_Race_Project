# Databricks notebook source
# MAGIC %md
# MAGIC #Step 1-- Read data file JSOn using spark data frame 

# COMMAND ----------

# MAGIC %fs ls abfss://raw@dbformuladl.dfs.core.windows.net/

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
             .load("abfss://raw@dbformuladl.dfs.core.windows.net/qualifying/"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Step 2- Transformatins of Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

renamed_qualify_df= qualifying_df. withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(renamed_qualify_df)

# COMMAND ----------

renamed_qualify_df.write.mode("overwrite").parquet("abfss://processed@dbformuladl.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@dbformuladl.dfs.core.windows.net/qualifying"))
