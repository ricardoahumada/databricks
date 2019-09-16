# Databricks notebook source
# Read Databricks switch action dataset  
from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime

events = spark.read \
  .option("inferSchema", "true") \
  .json("/databricks-datasets/structured-streaming/events/") \
  .withColumn("date", expr("time")) \
  .drop("time") \
  .withColumn("date", from_unixtime("date", 'yyyy-MM-dd'))
  
display(events)

# COMMAND ----------

# Write out DataFrame as Databricks Delta data
events.write.format("delta").mode("overwrite").partitionBy("date").save("/delta/events/")

# COMMAND ----------

#Query the data file path
events_delta = spark.read.format("delta").load("/delta/events/")

# COMMAND ----------

# create table
display(spark.sql("DROP TABLE IF EXISTS events"))

display(spark.sql("CREATE TABLE events USING DELTA LOCATION '/delta/events/'"))

# COMMAND ----------

events_delta.count()

# COMMAND ----------

from pyspark.sql.functions import count
display(events_delta.groupBy("action","date").agg(count("action").alias("action_count")).orderBy("date", "action"))

# COMMAND ----------

#Generate historical data - original data shifted backwards 2 days
historical_events = spark.read \
  .option("inferSchema", "true") \
  .json("/databricks-datasets/structured-streaming/events/") \
  .withColumn("date", expr("time-172800")) \
  .drop("time") \
  .withColumn("date", from_unixtime("date", 'yyyy-MM-dd'))

# COMMAND ----------

# append historical data
historical_events.write.format("delta").mode("append").partitionBy("date").save("/delta/events/")

# COMMAND ----------

display(events_delta.groupBy("action","date").agg(count("action").alias("action_count")).orderBy("date", "action"))

# COMMAND ----------

events_delta.count()

# COMMAND ----------

#Show contents of a partition
dbutils.fs.ls("dbfs:/delta/events/date=2016-07-25/")

# COMMAND ----------

display(spark.sql("OPTIMIZE events"))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY events"))

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL events"))

# COMMAND ----------

display(spark.sql("DESCRIBE FORMATTED events"))

# COMMAND ----------


