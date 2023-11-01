# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuit_df= spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

constructors_df= spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")

# COMMAND ----------

drivers_df= spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

results_df= spark.read.parquet(f"{processed_folder_path}/results") \
.filter((f"file_date ='{v_file_date}'"))\
.withColumnRenamed("time", "race_time").withColumnRenamed("race_id", "result_race_id").withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

races_df= spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #Join circuits to races 

# COMMAND ----------

races_circuits_df= races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuit_df.circuit_location)

# COMMAND ----------

# MAGIC %md 
# MAGIC #join all other destinations files
# MAGIC

# COMMAND ----------

race_results_df= results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) 

# COMMAND ----------

race_results_df.columns

# COMMAND ----------

final_df= race_results_df.select("race_year","race_id", "race_name", "race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date").transform(add_ingestion_data).withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

display(final_df)

# COMMAND ----------

overwrite_partation(final_df,'f1_presentation','race_results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results WHERE race_year= 2021;

# COMMAND ----------

