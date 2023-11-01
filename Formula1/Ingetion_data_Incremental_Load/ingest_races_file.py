# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingest Races_file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_function"

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
df= spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header= True, schema= df_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 2- Selecting the required columns 

# COMMAND ----------

from pyspark.sql.functions import col, lit

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
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_ate", lit(v_file_date))


display(df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4- Add ingestio date to dataframe
# MAGIC 1. Adding Current Time stamp
# MAGIC 2. Combining two colums ( Date + Time) and reanaming column as race_timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat, to_timestamp, concat_ws

# COMMAND ----------

final_df= df_renamed.withColumn("race_timestamp", to_timestamp(concat_ws(" ", df.date, df.time), "yyyy-MM-dd HH:mm:ss"))\
    .transform(add_ingestion_data)

display(final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 5-> Partittion of Data
# MAGIC -> Partation of race data based on race_year column
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("sucess")
