# Databricks notebook source
# MAGIC %md ####Ingestando el archivo circuits.csv

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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
              .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv") 

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
                      .withColumn("data_source", lit(v_data_source)) \
                      .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md **Paso 4 - Agregamos el campo fecha de carga al dataframe**

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md **Paso 5 - Escribir el DataFrame resultante al datalake como parquet**

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT * FROM f1_processed.circuits;
# MAGIC describe formatted f1_processed.circuits;
# MAGIC --DESC DETAIL f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(spark.read.parquet("/mnt/sa70903775/processed/circuits"))
