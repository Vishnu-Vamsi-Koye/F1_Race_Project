# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingest circuit_file
# MAGIC

# COMMAND ----------

#Importing Sql functions to Databricks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

#Declaring Schema to dataframe

df_schema= StructType(fields=[StructField("raceId", IntegerType(),False),
                              StructField("year", IntegerType(),True),
                              StructField("round", IntegerType(),True),
                              StructField("circuitId", IntegerType(),True),
                              StructField("name", StringType(),True),
                              StructField("date", DateType(),True),
                              StructField("time", StringType(),True),
                              StructField("url", StringType(),True)                             
])

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 1- Read the CSV file using Spark dataframe reader

# COMMAND ----------

#Readiing data using Clustered Autontication 
df= spark.read.csv("abfss://raw@dbformuladl.dfs.core.windows.net/races.csv", header= True, schema= df_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 2- Selecting the required columns 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------


Selected_df= df.select(col("raceId"),col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))
display(Selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 3-Renaming the columns

# COMMAND ----------

#Rename 
df_renamed= Selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("year", "race_year")\


display(df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4- Add ingestio date to dataframe
# MAGIC 1. Adding Current Time stamp
# MAGIC 2. Combining two colums ( Date + Time) and reanaming column as race_timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat, to_timestamp, concat_ws

# COMMAND ----------

final_df= df_renamed.withColumn("race_timestamp", to_timestamp(concat_ws(" ", df.date, df.time), "yyyy-MM-dd HH:mm:ss")).\
withColumn("ingestion_date", current_timestamp())

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 5-> Data Frame Writer to Parquet file formate
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@dbformuladl.dfs.core.windows.net/races")


# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 6-> Partittion of Data
# MAGIC -> Partation of race data based on race_year column
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy('race_year').parquet("abfss://processed@dbformuladl.dfs.core.windows.net/races")

# COMMAND ----------

# MAGIC %fs ls abfss://processed@dbformuladl.dfs.core.windows.net/races

# COMMAND ----------


