// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }
import org.apache.spark.sql.streaming.Trigger.ProcessingTime


// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val connectionString = ConnectionStringBuilder("Endpoint=sb://wordscount.servicebus.windows.net/;SharedAccessKeyName=AppEHAccess;SharedAccessKey=8wU/WZOYdm+UJsG35/9WkfjsweTpYqItHLMRO+hNt+g=")
  .setEventHubName("words")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

eventhubs.printSchema


// COMMAND ----------

val messages =
      eventhubs
      .withColumn("Offset", $"offset".cast("long"))
      .withColumn("Time (readable)", $"enqueuedTime".cast("timestamp"))
      .withColumn("Timestamp", $"enqueuedTime".cast("long"))
      .withColumn("Body", $"body".cast("string"))
      .select("Offset", "Time (readable)", "Timestamp", "Body")

eventhubs.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// split lines by whitespaces and explode the array as rows of 'word'
val df = eventhubs.select(explode(split($"body".cast("string"), "\\s+")).as("word"))
  .groupBy($"word")
  .count

// COMMAND ----------

// follow the word counts as it updates
display(df.select($"word", $"count"))
