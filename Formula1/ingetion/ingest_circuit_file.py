# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingest circuit_file
# MAGIC

# COMMAND ----------

#Importing Sql functions to Databricks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType  

# COMMAND ----------

#Declaring Schema to dataframe

df_schema= StructType(fields=[StructField("circuitId", IntegerType(),False),
                              StructField("circuitRef", StringType(),True),
                              StructField("name", StringType(),True),
                              StructField("location", StringType(),True),
                              StructField("country", StringType(),True),
                              StructField("lat", DoubleType(),True),
                              StructField("lng", DoubleType(),True),
                              StructField("alt", IntegerType(),True),
                              StructField("url", StringType(),True)                             
])

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 1- Read the CSV file using Spark dataframe reader

# COMMAND ----------

#Readiing data using Clustered Autontication 
df= spark.read.csv("abfss://raw@dbformuladl.dfs.core.windows.net/circuits.csv", header= True, schema=df_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 2- Selecting the required columns 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md Types_of_selects

# COMMAND ----------

#Type 1
Selected_df1= df.select("circuitId","circuitRef", "name", "location", "country", "lat", "lng","alt")
display(Selected_df1)

# COMMAND ----------

#Type 2
Selected_df2= df.select(df.circuitId,df.circuitRef, df.name, df.location, df.country, df.lat, df.lng, df.alt)
display(Selected_df2)

# COMMAND ----------

#Type 3
Selected_df3= df.select(df["circuitId"],df["circuitRef"], df["name"], df["location"], df["country"], df["lat"], df["lng"],df["alt"])
display(Selected_df3)

# COMMAND ----------

#Type 4
Selected_df4= df.select(col("circuitId"),col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"),col("alt"))
display(Selected_df4)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 3-Renaming the columns

# COMMAND ----------

#Rename 
df_renamed= Selected_df4.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")

display(df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4- Add ingestio date to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df= df_renamed. withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 5-> Data Frame Writer
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://processed@dbformuladl.dfs.core.windows.net/Circuits/circuits_processed.parquet")

