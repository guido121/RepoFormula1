# Databricks notebook source
# MAGIC %md ####Ingestando el archivo races.csv

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

# MAGIC %run "./includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer el archivo csv usando el Reader de Spark Dataframe API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                ])

# COMMAND ----------

races_df = spark.read \
  .option("header",True) \
  .schema(races_schema) \
  .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md **Paso 2 - Agregar los campos ingestion_date y race_timestamp to the dataframe**

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, col

# COMMAND ----------

races_with_timestamp_df = races_with_ingestion_date_df.withColumn(
                                                                  "race_timestamp",
                                                                  to_timestamp(
                                                                    concat(
                                                                      col('date'),
                                                                      lit(' '),
                                                                      col('time')
                                                                    ),
                                                                    'yyyy-MM-dd HH:mm:ss'
                                                                  )
                                                        ) \
                                                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md **Paso 3 - Selecciona solo las columnas requeridas**

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),
                                                   col('year').alias('race_year'),
                                                   col('round'),
                                                   col('circuitId').alias('circuit_id'),
                                                   col('name'),
                                                   col('ingestion_date'),
                                                   col('race_timestamp')
                                                   )

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
