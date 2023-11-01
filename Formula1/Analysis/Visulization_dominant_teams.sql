-- Databricks notebook source
-- MAGIC %python
-- MAGIC html="""<h1 style="color:Black; text-align:center; font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visulization Dominant_Teams

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_team
AS
SELECT team_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points,
rank() OVER (ORDER BY round(avg(calculated_points),2) DESC) as team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1)>=50
ORDER BY  Average_points DESC

-- COMMAND ----------

SELECT race_year, team_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_team WHERE team_rank<=10) 
GROUP BY race_year, team_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### creating line plot for dominated teams over years 

-- COMMAND ----------

SELECT race_year, team_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_team WHERE team_rank<=10) 
GROUP BY race_year, team_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####creating Area plot to know dominated teams over past 10 years 

-- COMMAND ----------

SELECT race_year, team_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_team WHERE team_rank<=10) And race_year BETWEEN 2011 AND 2020
GROUP BY race_year, team_name
ORDER BY race_year, Average_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####creating Bar plot to know atotal races and total points that top 10 teams made

-- COMMAND ----------

SELECT race_year, team_name, count(1) AS Total_races,
SUM(calculated_points) AS Total_Points,
round(avg(calculated_points),2) As Average_points
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name from v_dominant_team WHERE team_rank<=10) 
GROUP BY race_year, team_name
ORDER BY race_year, Average_points DESC
