# Databricks notebook source
# MAGIC %md
# MAGIC **Acceder a Azure Data Lake utilizando Service Principal**
# MAGIC
# MAGIC **Pasos a seguir**
# MAGIC 1. Obtener el client_id, tenant_id y client_secret desde Key Vault.
# MAGIC 1. Asignar la configuración de Spark con App/Client Id, Directory/ Tenant Id & Secretos.
# MAGIC 1. Invocar al utilitario de sistema de archivos **mount** para añadirlo al almacenamiento.
# MAGIC 1. Explorar los otros utilitarios de sistemas de archivos  relacionados con **mount** (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'scope-70903775', key = 'app-client-id')
tenant_id = dbutils.secrets.get(scope = 'scope-70903775', key = 'app-tenant-id')
client_secret= dbutils.secrets.get(scope = 'scope-70903775', key = 'app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@sa70903775.dfs.core.windows.net/",
  mount_point = "/mnt/sa70903775/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/sa70903775/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/sa70903775/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/sa70903775/demo')

# COMMAND ----------


