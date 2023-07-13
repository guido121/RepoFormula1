# Databricks notebook source
# MAGIC %md ###Ingestando el archivo constructors.json file

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo JSON usando Spark DataFrame Reader**

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema) \
                            .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md **Paso 2 - Borrar columnas no deseadas del DataFrame**

# COMMAND ----------

from pyspark.sql.functions import col

constructor_dropped_df = constructor_df.drop(col('url'))


# COMMAND ----------

# MAGIC %md **Paso 3 - Renombrar columnas y agregar columna ingestion_date**

# COMMAND ----------

constructor_ingestion_date_df = add_ingestion_date(constructor_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = constructor_ingestion_date_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC **Paso 4 - Escribir resultado a un archivo parquet**

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
