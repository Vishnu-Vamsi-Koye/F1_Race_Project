-- Databricks notebook source
DROP DATABASE f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@dbformuladl.dfs.core.windows.net/"

-- COMMAND ----------

DROP DATABASE f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@dbformuladl.dfs.core.windows.net/"

-- COMMAND ----------


