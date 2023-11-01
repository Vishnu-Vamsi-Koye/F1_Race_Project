-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:Black; text-align:center; font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Visulization Dominant_drivers 
-- MAGIC

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_driver
AS
SELECT driver_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points,
rank() OVER (ORDER BY round(avg(calculated_points),2) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY  Average_points DESC

-- COMMAND ----------

SELECT race_year, driver_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_driver WHERE driver_rank<=10) 
GROUP BY race_year, driver_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####creating line plot for dominated drivers over years 

-- COMMAND ----------

SELECT race_year, driver_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_driver WHERE driver_rank<=10) 
GROUP BY race_year, driver_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####creating Area plot to know dominated drivers over past 10 years 

-- COMMAND ----------

SELECT race_year, driver_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_driver WHERE driver_rank<=10) And race_year BETWEEN 2011 AND 2020
GROUP BY race_year, driver_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####creating Bar plot to know atotal races and total points that top 10 drivers made
-- MAGIC

-- COMMAND ----------

SELECT race_year, driver_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name from v_dominant_driver WHERE driver_rank<=10) 
GROUP BY race_year, driver_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------


