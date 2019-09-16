# Databricks notebook source
# Read Configuration
readConfig = {
  "Endpoint" : "https://doctorwho.documents.azure.com:443/",
  "Masterkey" : "<key>",
  "Database" : "DepartureDelays",
  "preferredRegions" : "Central US;East US2",
  "Collection" : "flights_pcoll",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "1000",
  "query_pagesize" : "2147483647",
  "query_custom" : "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
}


# COMMAND ----------

# Connect via azure-cosmosdb-spark to create Spark DataFrame
flights = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()
flights.count()

# COMMAND ----------

# Write configuration
writeConfig = {
    "Endpoint": "https://data-cosmos-db.documents.azure.com:443/",
    "Masterkey": "<key>",
    "Database": "DepartureDelays",
    "Collection": "flights",
    "Upsert": "true"
}

# Write to Cosmos DB from the flights DataFrame
flights.write.format("com.microsoft.azure.cosmosdb.spark").options(**writeConfig).save()

# COMMAND ----------

# Read Configuration
readConfig = {
  "Endpoint": "https://data-cosmos-db.documents.azure.com:443/",
  "Masterkey": "<key>",
  "Database": "DepartureDelays",
  "Collection": "flights",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "1000",
  "query_pagesize" : "2147483647",
  "query_custom" : "SELECT c.date, c.delay, c.distance, c.origin, c.destination FROM c WHERE c.origin = 'SEA'"
}

my_flights = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()

# COMMAND ----------

display(my_flights)
