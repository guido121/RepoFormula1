# Databricks notebook source
# MAGIC %md ####Access dataframes using SQL
# MAGIC
# MAGIC **Objectives**
# MAGIC 1. Create temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md ####Temp View

# COMMAND ----------

#race_results_df.createTempView("v_race_results")
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")
race_results_2019_df.display()

# COMMAND ----------

# MAGIC %md ####Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from Python cell
# MAGIC 1. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")
#race_results_df.createGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show(truncate=False)
