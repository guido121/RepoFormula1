# Databricks notebook source
def mount_container(storage_account,container):
    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
        mount_point = f"/mnt/{storage_account}/{container}",
        extra_configs = {f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net": "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-07-18T08:56:57Z&st=2023-07-18T00:56:57Z&spr=https&sig=4ulXuzAGcU6TIKzHy7F7g5oVcMM3LOTxvxoGXBLSC%2F4%3D"}
    )
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_container("sa70903775","raw")

# COMMAND ----------

mount_container("sa70903775","processed")

# COMMAND ----------

mount_container("sa70903775","presentation")
