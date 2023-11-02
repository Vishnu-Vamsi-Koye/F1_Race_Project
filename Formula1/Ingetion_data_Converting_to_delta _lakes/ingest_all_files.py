# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/1. ingest_circuit_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/2. ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/3. ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/4. Ingest_drivers_File", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/5. ingest_results_File", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/6. Ingest_laptimes_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/7. ingest_pit_stop_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("/Repos/lvemula1s@semo.edu/F1_Race_Project/Formula1/Ingetion_data_Converting_to_delta _lakes/8. ingest_Qualifying_files", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

v_result
