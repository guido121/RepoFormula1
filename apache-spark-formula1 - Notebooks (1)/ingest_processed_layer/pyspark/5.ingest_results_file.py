# Databricks notebook source
# MAGIC %md ####Ingest result.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","formula 1")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo JOSN usando el Reader de Spark API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
                                      StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", FloatType(), True),
                                      StructField("statusId", StringType(), True)
                                    ])

# COMMAND ----------

results_df =  spark.read \
  .schema(results_schema) \
  .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar columnas nuevas**

# COMMAND ----------

results_ingestion_date_df = add_ingestion_date(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_ingestion_date_df.withColumnRenamed("resultId","result_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("constructorId","constructor_id") \
                                    .withColumnRenamed("positionText","position_text") \
                                    .withColumnRenamed("positionOrder","position_order") \
                                    .withColumnRenamed("fastestLap","fastest_lap") \
                                    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md **Paso 3 - Eliminar columnas no deseadas**

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md **Paso 4 - Escribir resultado en un archivo parquet**

# COMMAND ----------

# MAGIC %md Metodo 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")): 
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id={race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md Metodo 2

# COMMAND ----------

overwrite_partition(results_final_df,'f1_processed', 'results', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
