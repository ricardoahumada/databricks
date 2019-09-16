# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://databricks-blob@databricksstoragerit.blob.core.windows.net",
  mount_point = "/mnt/blob-mount",
  extra_configs = {"fs.azure.account.key.databricksstoragerit.blob.core.windows.net":"0UxjblePDg4vuPdjewHv/1uB4OZgXMsr50zPO2LOHX4b61jOnCkSHuSfkgOI9Rm2CZj5zm4oO/akVW3pfz1xvA=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/blob-mount")

# COMMAND ----------

dbutils.fs.unmount("/mnt/blob-mount")
