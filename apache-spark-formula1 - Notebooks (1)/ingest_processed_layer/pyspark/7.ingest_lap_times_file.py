# Databricks notebook source
# MAGIC %md ####Ingestar los archivos lap_times

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer archivo CSV usando Spark DataFrame Reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
                                        StructField("raceId",IntegerType(), False),
                                        StructField("driverId",IntegerType(), True),
                                        StructField("lap",IntegerType(), True),
                                        StructField("position",IntegerType(), True),
                                        StructField("time",StringType(), True),
                                        StructField("milliseconds",IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar nuevas columnas**
# MAGIC 1. Renombrar driverId y raceId
# MAGIC 1. Agregar el campo ingestion_date y current timestamp

# COMMAND ----------

lap_times_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = lap_times_ingestion_date_df.withColumnRenamed("driverId","driver_id") \
                                      .withColumnRenamed("raceId", "race_id") \
                                      .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md **Paso 3 - Escribir el resultado en un archivo Parquet**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/laptimes")

# COMMAND ----------

dbutils.notebook.exit("Success")
