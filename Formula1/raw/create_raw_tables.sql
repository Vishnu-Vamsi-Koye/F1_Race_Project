-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create circuit tables 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
( circuitId INT, 
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING CSV
OPTIONS(path "abfss://raw@dbformuladl.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Create Races Table 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
raceId INT, 
year INT,
round INT,
circuitID INT, 
name STRING, 
date DATE, 
time STRING, 
url STRING
)
USING CSV
OPTIONS(path "abfss://raw@dbformuladl.dfs.core.windows.net/races.csv", header True)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Tables from JSON files
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create constructor Table
-- MAGIC 1. Single line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
options(path "abfss://raw@dbformuladl.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create drivers Table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
  driverId INT, 
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename : STRING, surname: STRING>,
  dob DATE,
  nationality STRING, 
  url STRING
)
USING JSON
options(path "abfss://raw@dbformuladl.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Results Table 
-- MAGIC 1. Single JSON file
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
options (path "abfss://raw@dbformuladl.dfs.core.windows.net/results.json")

-- COMMAND ----------

select * from f1_raw.results;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create Pit Stop table
-- MAGIC 1. Multi Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "abfss://raw@dbformuladl.dfs.core.windows.net/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Create Tables from list of Files 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create Lap times Table
-- MAGIC 1. CSV File
-- MAGIC 2. Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@dbformuladl.dfs.core.windows.net/lap_times/")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId IN
)
USING json
OPTIONS (path "abfss://raw@dbformuladl.dfs.core.windows.net/qualifying/", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;
