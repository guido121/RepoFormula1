-- Databricks notebook source
-- MAGIC %md **Objetivos de la lección**
-- MAGIC 1. Spark SQ documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;


-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

-- MAGIC %md ####MANAGED TABLES

-- COMMAND ----------

-- MAGIC %md **Learning Objectives**
-- MAGIC 1. Create a managed table using Python
-- MAGIC 1. Create a managed able using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe a table

-- COMMAND ----------

-- MAGIC %run "./../../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;


-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md ####EXTERNAL TABLES

-- COMMAND ----------

-- MAGIC %md **Learning Objectives**
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect dropping in external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/formuladl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md **Permanent View**

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2000;


-- COMMAND ----------

SHOW TABLES IN demo;
