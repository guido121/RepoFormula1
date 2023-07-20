# Databricks notebook source
def mount_container(storage_account,container):
    mount_point = f"/mnt/{storage_account}/{container}"
    mounts = dbutils.fs.mounts()
    if any(mount.mountPoint == mount_point for mount in mounts):
        dbutils.fs.unmount(mount_point)

    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
        mount_point = f"/mnt/{storage_account}/{container}",
        extra_configs = {f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net": "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-07-20T06:46:46Z&st=2023-07-19T22:46:46Z&spr=https&sig=erI%2FUopaRMeaybudIORtqZA8uFUnLcjeTMGV0Ahjxe8%3D"}
    )
    print(f"{mount_point} has been mounted.")
    display(dbutils.fs.mounts())
    

# COMMAND ----------

mount_container("sa70903775","demo")

# COMMAND ----------

mount_container("sa70903775","processed")

# COMMAND ----------

mount_container("sa70903775","presentation")

# COMMAND ----------

dbutils.fs
