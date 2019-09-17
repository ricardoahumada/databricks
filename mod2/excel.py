# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "6987cd8a-481d-46ac-bd87-a0b1f13fffa2",
"fs.azure.account.oauth2.client.secret": "j=Y9:u_+oOxP50sYq4?MQF*3Tnj35qCM",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9452b23e-fd00-40c5-98f7-d9a67ab13a7e/oauth2/token"}

dbutils.fs.mount(
source = "abfss://databricks@databrickssadl.dfs.core.windows.net/data",
mount_point = "/mnt/data-lake",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/data-lake")

# COMMAND ----------

# MAGIC %scala
# MAGIC val titanic_df = spark.read.format("com.crealytics.spark.excel")
# MAGIC .option("useHeader", "true") // Required
# MAGIC .load("/mnt/data-lake/1_titanic3.xls")

# COMMAND ----------

# MAGIC %scala
# MAGIC titanic_df.createOrReplaceTempView("titanic_df")

# COMMAND ----------

titanic_df=spark.table("titanic_df")
titanic_df.printSchema()

# COMMAND ----------

display(titanic_df)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/data-lake") 
