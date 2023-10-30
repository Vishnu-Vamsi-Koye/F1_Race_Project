# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result= dbutils.notebook.run("ingest_circuit_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("ingest_constructors_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("Ingest_laptimes_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("ingest_pit_stop_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("ingest_Qualifying_files", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("ingest_races_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("ingest_results_File", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run("Ingest_drivers_File", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
