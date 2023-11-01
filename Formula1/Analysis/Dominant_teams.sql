-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #Dominant_teams 
-- MAGIC

-- COMMAND ----------

select team_name from f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT team_name,count(*) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points,
RANK() OVER(ORDER BY round(avg(calculated_points),2) DESC ) as team_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING count(*)>=100
ORDER BY Total_Points DESC, Average_points DESC

-- COMMAND ----------

SELECT team_name,count(*) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points,
RANK() OVER(ORDER BY round(avg(calculated_points),2) DESC ) as team_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING count(*)>=100
ORDER BY Total_Points DESC, Average_points DESC

-- COMMAND ----------


