# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race years for which the data is to be processed

# COMMAND ----------

race_results_list=spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .filter(f"result_file_date='{v_file_date}'")\
    .select("race_year")\
    .distinct()\
    .collect()

# COMMAND ----------

race_year_list=[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when,col, count

# COMMAND ----------

drivers_standing_df= race_results_df \
.groupBy("race_year", "driver_name","driver_nationality","team")\
.agg(sum("points").alias("total_points") ,count(when(col("position")==1, True)).alias("wins")
)

# COMMAND ----------

display(drivers_standing_df)

# COMMAND ----------

from pyspark.sql.functions import desc , rank
from pyspark.sql.window import Window

# COMMAND ----------

driver_rank= Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=drivers_standing_df.withColumn("rank", rank().over(driver_rank)) 

# COMMAND ----------


overwrite_partation(final_df, 'f1_presentation','driver_standings','race_year')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_presentation.driver_standings WHERE race_year=2021;
