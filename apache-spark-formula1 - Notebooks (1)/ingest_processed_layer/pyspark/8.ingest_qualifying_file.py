# Databricks notebook source
# MAGIC %md ####Ingesta los archivos json qualifying

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer los archivos JSON usando Spark Reader API**

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[
                                        StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                      ]
                               )

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar columnas**
# MAGIC 1. Renombrar qualifyingId, driverId, constructorId y raceId
# MAGIC 1. Añadir campo ingestion_date con current timestamp

# COMMAND ----------

qualifying_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = qualifying_ingestion_date_df.withColumnRenamed("qualifyId","qualify_id") \
                        .withColumnRenamed("driverId","driver_id") \
                        .withColumnRenamed("raceId","race_id") \
                        .withColumnRenamed("constructorId","constructor_id") \
                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md **Paso 3 - Escribir el resultado en un archivo Parquet**

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
