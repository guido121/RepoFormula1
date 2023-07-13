# Databricks notebook source
# MAGIC %md ####Ingestar el archivo pit_stops.json

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo JSON usango el Spark Reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pits_stops_schema = StructType(fields=[
                                        StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), False),
                                        StructField("stop", StringType(), False),
                                        StructField("lap", IntegerType(), False),
                                        StructField("time", StringType(), False),
                                        StructField("duration", StringType(), False),
                                        StructField("millisecond", IntegerType(), False)
                                      ])

# COMMAND ----------

pits_stops_df = spark.read \
.schema(pits_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar columnas**
# MAGIC 1. Renombrar driverId y raceId
# MAGIC 1. Añadir el campo ingestion_date with curent timestamp

# COMMAND ----------

pits_stops_ingestion_date_df = add_ingestion_date(pits_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df =  pits_stops_ingestion_date_df.withColumnRenamed("driverId","driver_id") \
                                        .withColumnRenamed("raceId","race_id") \
                                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md **Paso 3 - Escribir el resultado en un archivo parquet**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
