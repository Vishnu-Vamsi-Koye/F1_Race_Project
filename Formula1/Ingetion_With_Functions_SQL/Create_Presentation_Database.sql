-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@dbformuladl.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE f1_presentation;
