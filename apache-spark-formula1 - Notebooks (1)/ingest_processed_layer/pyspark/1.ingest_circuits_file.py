# Databricks notebook source
# MAGIC %md ####Ingestando el archivo circuits.csv

# COMMAND ----------

# dbutils.widgets.help()

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo CSV usando el reader de Spark**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
              .option("header",True)\
              .schema(circuits_schema)\
              .csv(f"{raw_folder_path}/circuits.csv") 

# COMMAND ----------

# MAGIC %md **Paso 2 - Seleccionando columnas necesarias**

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(
                        col("circuitId"),
                        col("circuitRef"),
                        col("name"),
                        col("location"),
                        col("country").alias("race_country"),
                        col("lat"),
                        col("lng"),
                        col("alt")
                      )

# COMMAND ----------

# MAGIC %md **Paso 3 - Renombrando las columnas de acuerdo a lo que se necesite**

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
                      .withColumnRenamed("circuitRef","circuit_ref") \
                      .withColumnRenamed("lat","latitude") \
                      .withColumnRenamed("lng","longitude") \
                      .withColumnRenamed("alt","altitude") \
                      .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md **Paso 4 - Agregamos el campo fecha de carga al dataframe**

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md **Paso 5 - Escribir el DataFrame resultante al datalake como parquet**

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
