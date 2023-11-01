-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #Dominant_drivers 
-- MAGIC

-- COMMAND ----------

SELECT driver_name, count(*) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points,
RANK() OVER(ORDER BY round(avg(calculated_points),2) DESC ) as driver_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING Total_races>=25
ORDER BY Total_Points DESC, Average_points DESC

-- COMMAND ----------

SELECT driver_name, count(*) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points,
RANK() OVER(ORDER BY round(avg(calculated_points),2) DESC ) as driver_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY driver_name
HAVING count(*)>=50
ORDER BY Total_Points DESC, Average_points DESC

-- COMMAND ----------


