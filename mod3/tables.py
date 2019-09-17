# Databricks notebook source
# MAGIC %sql
# MAGIC USE default;

# COMMAND ----------

rental = spark.table("bike_rental_uci_dataset_bb6c6_csv")
display(rental.select("*"))

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE bike_rental_uci_dataset_bb6c6_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE bike_rental_uci_dataset_bb6c6_csv;
