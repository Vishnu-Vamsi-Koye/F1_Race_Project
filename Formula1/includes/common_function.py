# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_data(input_df):
    output_df=input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Increment Load Function
# MAGIC

# COMMAND ----------

def re_arrange_partations_column(input_df,partition_column):
    column_list=[]
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df=input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partation(input_df, db_name,table_name, partation_column):
  output_df=re_arrange_partations_column(input_df,partation_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if(spark.catalog.tableExists(f"{db_name}.{table_name}")):
      output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
      output_df.write.mode("overwrite").partitionBy(partation_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------


