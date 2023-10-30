# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingest circuit_file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

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
df= spark.read.csv(f"{raw_folder_path}/circuits.csv", header= True, schema=df_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 2- Selecting the required columns 

# COMMAND ----------

from pyspark.sql.functions import col, lit

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
.withColumnRenamed("alt", "altitude")\
.withColumn("data_source", lit(v_data_source))

display(df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4- Add ingestio date to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df= add_ingestion_data(df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 5-> Data Frame Writer
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")


# COMMAND ----------

dbutils.notebook.exit("sucess")
