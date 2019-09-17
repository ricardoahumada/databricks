# Databricks notebook source
# MAGIC %md ## Part A: Load & Transform Data
# MAGIC 
# MAGIC In this first stage we are going to load some distributed data, read that data as an RDD, do some transformations on that RDD, construct a Spark DataFrame from that RDD and register it as a table.

# COMMAND ----------

# MAGIC %md ###### A1. List files 
# MAGIC You can list files on a distributed file system (DBFS, S3 or HDFS) using `%fs` commands.
# MAGIC 
# MAGIC For this example, we are using data files stored in DBFS at `dbfs:/databricks-datasets/songs/data-001`. DBFS is the Databricks File System that leverages AWS S3 and the SSD drives attached to Spark clusters hosted in AWS. When accessing a file, it first checks if file is cached in the SSD drive, then, if unavailable, goes out to the specific S3 bucket to get the file(s).

# COMMAND ----------

# MAGIC 
# MAGIC %fs ls /databricks-datasets/songs/data-001/

# COMMAND ----------

# MAGIC %md ######A2. Display contents of header 
# MAGIC In the listing we have data files and a single header file. Let's use the Pyspark `textFile` command to read the content of the header file then use `collect` to display the contents. After running this, we will see that the header consists of a name and a type, separated by colon

# COMMAND ----------

sc.textFile("databricks-datasets/songs/data-001/header.txt").collect()

# COMMAND ----------

# MAGIC %md ######A3. Examine a data file
# MAGIC Let's use the pyspark `textFile` command to load one of the data files, then use the pyspark `take` command to view the first 3 lines of the data. After running this, you will see each line consists of multiple fields separated by a `\t`.

# COMMAND ----------

dataRDD = sc.textFile("/databricks-datasets/songs/data-001/part-000*")
dataRDD.take(3)

# COMMAND ----------

# MAGIC %md ###### A4. Create python function to parse fields
# MAGIC Give what we now know about the data, we set out to parse it. To do so, we build a function that takes a line of text and returns an array of parsed fields.
# MAGIC * If header indicates the type is int, we cast the token to integer
# MAGIC * If header indicates the type is double, we cast the token to float
# MAGIC * Otherwise we return the string

# COMMAND ----------

#Split the header by its separator
header = sc.textFile("/databricks-datasets/songs/data-001/header.txt").map(lambda line: line.split(":")).collect()

#Create the Python function
def parseLine(line):
  tokens = zip(line.split("\t"), header)
  parsed_tokens = []
  for token in tokens:
    token_type = token[1][1]
    if token_type == 'double':
      parsed_tokens.append(float(token[0]))
    elif token_type == 'int':
      parsed_tokens.append(-1 if '-' in token[0] else int(token[0])) # Taking care of fields with --
    else:
      parsed_tokens.append(token[0])
  return parsed_tokens

# COMMAND ----------

# MAGIC %md ####### A5. Convert header structure
# MAGIC Before using our parsed header, we need to convert it to the type that SparkSQL expects. That entails using SQL types (`IntegerType`, `DoubleType`, and `StringType`) and using `StructType` instead of a normal python list.

# COMMAND ----------

from pyspark.sql.types import *

def strToType(str):
  if str == 'int':
    return IntegerType()
  elif str == 'double':
    return DoubleType()
  else:
    return StringType()

schema = StructType([StructField(t[0], strToType(t[1]), True) for t in header])

# COMMAND ----------

# MAGIC %md ###### A6. Create a DataFrame 
# MAGIC We use Spark's `createDataFrame` method to combine the schema information and the parsed data to construct a DataFrame. DataFrames are typically preferred due to easier manipulation and because Spark knows about the types of data, can do a better job processing them. Most of Spark's libraries (such as SparkML) take their input in the form of DataFrames.

# COMMAND ----------

df = sqlContext.createDataFrame(dataRDD.map(parseLine), schema)


# COMMAND ----------

# MAGIC %md ###### A7. Create a Temp Table
# MAGIC Now that we have a DataFrame, we can register it as a temporary table. That will allow us to use its name in SQL queries.

# COMMAND ----------

df.registerTempTable("songsTable")

# COMMAND ----------

# MAGIC %md ###### A8. Cache the table
# MAGIC Because we are going to access this data multiple times, let's cache it in memory for faster subsequent access.

# COMMAND ----------

# MAGIC %sql cache table songsTable

# COMMAND ----------

# MAGIC %md ###### A9. Query the data 
# MAGIC From now on we can easily query our data using the temporary table we just created and cached in memory. Since it is registered as a table we can use SQL as well as Spark API to access it.

# COMMAND ----------

# MAGIC %sql select * from songsTable limit 15

# COMMAND ----------

# MAGIC %md ##Part B: Explore & Visualize The Data

# COMMAND ----------

# MAGIC %md ###### B1. Display the table schema

# COMMAND ----------

table("songsTable").printSchema()

# COMMAND ----------

# MAGIC %md ###### B2. Get count of rows In table

# COMMAND ----------

# MAGIC %sql select count(*) from songsTable

# COMMAND ----------

# MAGIC %md ###### B3. Visualize a data point: Song duration changes over time
# MAGIC An interesting question is how different parameters of songs change over time. For example, how did average song durations change over time?
# MAGIC 
# MAGIC We begin by importing in [ggplot](http://ggplot2.org/), which makes plotting data really easy in python. Next, we put together the sql query that will pull the necessary data from the table. From there, we create a Pandas data frame object (`toPandas()`), and use the `display` method to render the graph.

# COMMAND ----------

from ggplot import *
baseQuery = sqlContext.sql("select avg(duration) as duration, year from songsTable group by year")
df_filtered = baseQuery.filter(baseQuery.year > 0).filter(baseQuery.year < 2010).toPandas()
plot = ggplot(df_filtered, aes('year', 'duration')) + geom_point() + geom_line(color='blue')
display(plot)
