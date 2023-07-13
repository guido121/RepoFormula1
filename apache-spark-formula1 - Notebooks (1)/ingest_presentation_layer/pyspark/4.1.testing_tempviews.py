# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM v_race_results WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM global_temp.gv_race_results;
