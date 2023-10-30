# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", " ")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 1--> Read the JSON file using spark dataframe reader API
# MAGIC 1. Import required functions from pyspark.sql library 
# MAGIC 2. Define schema using struct type and struct fields
# MAGIC 3. Load the data from raw folder

# COMMAND ----------

#Importing Sql functions to Databricks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType, DateType

# COMMAND ----------

#Given data fle has nested json file formate, so we need to develop schema for every loop and need to call in main schema

name_schema= StructType(fields=[StructField("forename", StringType(), True),
                                StructField("surname", StringType(), True)
                                
])

# COMMAND ----------

# Developing new schema for drivers data and calling nested lop "name", in this main schema

drivers_schema= StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name",name_schema),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True)
                           
                           
])

# COMMAND ----------

# Read the Json file
driver_df = (spark
             .read
             .format('json') # Declearing file Formate
             .schema(drivers_schema) #  Declearing schema that we developed
             .load(f"{raw_folder_path}/drivers.json"))

# COMMAND ----------

display(driver_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 2-- Rename the columns and add new columns 
# MAGIC 1. driverid renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatentation of forname and surname
# MAGIC 5. drop unwanted columns like url forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_transformed_df= driver_df.withColumnRenamed("driverid", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).transform(add_ingestion_data).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(drivers_transformed_df)

# COMMAND ----------

#droping unwanted columns 
Final_drivers_df=drivers_transformed_df.drop(col("url"))

# COMMAND ----------

display(Final_drivers_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 3-- Write data into processed container in parquet format 

# COMMAND ----------

Final_drivers_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("sucess")
