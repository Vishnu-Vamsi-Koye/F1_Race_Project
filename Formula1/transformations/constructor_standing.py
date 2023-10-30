# Databricks notebook source
# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/configuration

# COMMAND ----------

# MAGIC %run /Users/lvemula1s@semo.edu/Vishnu/Formula1/includes/common_function

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when,col, count

# COMMAND ----------

constructor_standing_df= race_results_df \
.groupBy("race_year","team")\
.agg(sum("points").alias("total_points") ,count(when(col("position")==1, True)).alias("wins")
)

# COMMAND ----------

display(constructor_standing_df)

# COMMAND ----------

from pyspark.sql.functions import desc , rank
from pyspark.sql.window import Window

# COMMAND ----------

constructor_rank= Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructor_standing_df.withColumn("rank", rank().over(constructor_rank)) 

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
