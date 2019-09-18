# Databricks notebook source
# MAGIC %sql show databases

# COMMAND ----------

# MAGIC %sql 
# MAGIC use default;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql drop table events

# COMMAND ----------

# MAGIC %sql show tables;

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table cleaned_taxes
# MAGIC drop table dimbrandsdlta
# MAGIC drop table site_temperatures

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql CLEAR CACHE

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# MAGIC %scala
# MAGIC for ((k,v) <- sc.getPersistentRDDs) {
# MAGIC   print("unpersisting: "+k+"::"+v+"...")
# MAGIC   //v.unpersist()
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC def unpersistTable(tid : Integer) = {  
# MAGIC   for ((k,v) <- sc.getPersistentRDDs) {
# MAGIC     if(k==tid){
# MAGIC       print("unpersisting: "+k+"::"+v+"...")
# MAGIC       v.unpersist()
# MAGIC     }
# MAGIC   }
# MAGIC } 

# COMMAND ----------

# MAGIC %scala
# MAGIC unpersistTable(188)
