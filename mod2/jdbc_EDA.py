# Databricks notebook source
# MAGIC %scala
# MAGIC // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC // Class.forName("org.mariadb.jdbc.Driver")
# MAGIC Class.forName("com.mysql.jdbc.Driver")

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcHostname = "mysql-titanic.mysql.database.azure.com"
# MAGIC val jdbcPort = 3306
# MAGIC val jdbcDatabase = "titanic"
# MAGIC 
# MAGIC val jdbcUsername="ricardo@mysql-titanic"
# MAGIC val jdbcPassword="Formacion2019"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")

# COMMAND ----------

# MAGIC %scala
# MAGIC val titanic_df = spark.read.jdbc(jdbcUrl, "data", connectionProperties)
# MAGIC titanic_df.createOrReplaceTempView("titanic_df")

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.DriverManager
# MAGIC val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC connection.isClosed()
# MAGIC connection.close()

# COMMAND ----------

titanic_df=spark.table("titanic_df")
titanic_df.show()

# COMMAND ----------

titanic_df.printSchema

# COMMAND ----------

# MAGIC %md ##Let's do some simple exploratory data analysis (EDA)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

# Knowing the number of Passengers Survived ?
titanic_df.groupBy("Survived").count().show()
gropuBy_output = titanic_df.groupBy("Survived").count()
display(gropuBy_output)

# COMMAND ----------

# To know the particulars about survivors we have to explore more of the data.
# The survival rate can be determined by different features of the dataset such as Sex, Port of Embarcation, Age; few to be mentioned. Checking survival rate using feature Sex

titanic_df.groupBy("Sex","Survived").count().show()

# COMMAND ----------

# Although the number of males are more than females on ship, the female survivors are twice the number of males saved.
titanic_df.groupBy("Pclass","Survived").count().show()

# COMMAND ----------

# Checking Null values
# This function use to print feature with null values and null count 
def null_value_count(df):
  null_columns_counts = []
  numRows = df.count()
  for k in df.columns:
    nullRows = df.where(col(k).isNull()).count()
    if(nullRows > 0):
      temp = k,nullRows
      null_columns_counts.append(temp)
  return(null_columns_counts)

# Calling function
null_columns_count_list = null_value_count(titanic_df)

spark.createDataFrame(null_columns_count_list, ['Column_With_Null_Value', 'Null_Values_Count']).show()


# COMMAND ----------

mean_age = titanic_df.select(mean('Age')).collect()[0][0]
print(mean_age)

# COMMAND ----------

titanic_df.select("Name").show()

# COMMAND ----------

#To replace these NaN values, we can assign them the mean age of the dataset.But the problem is, there were many people with many different #ages. We just cant assign a 4 year kid with the mean age that is 29 years.

#we can check the Name feature. Looking upon the feature, we can see that the names have a salutation like Mr or Mrs. Thus we can assign the mean values of Mr and Mrs to the respective groups

titanic_df = titanic_df.withColumn("Initial",regexp_extract(col("Name"),"([A-Za-z]+)\.",1))

# Using the Regex ""[A-Za-z]+)." we extract the initials from the Name. It looks for strings which lie between A-Z or a-z and followed by a .(dot).


# COMMAND ----------

titanic_df.show()

# COMMAND ----------

titanic_df.select("Initial").distinct().show()


# COMMAND ----------

# There are some misspelled Initials like Mlle or Mme that stand for Miss. I will replace them with Miss and same thing for other values.

titanic_df = titanic_df.replace(['Mlle','Mme', 'Ms', 'Dr','Major','Lady','Countess','Jonkheer','Col','Rev','Capt','Sir','Don'],
               ['Miss','Miss','Miss','Mr','Mr',  'Mrs',  'Mrs',  'Other',  'Other','Other','Mr','Mr','Mr'])

titanic_df.select("Initial").distinct().show()

# COMMAND ----------

# lets check the average age by Initials
titanic_df.groupby('Initial').avg('Age').collect()

#Let's impute missing values in age feature based on average age of Initials

titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Miss") & (titanic_df["Age"].isNull()), 22).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Other") & (titanic_df["Age"].isNull()), 46).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Master") & (titanic_df["Age"].isNull()), 5).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Mr") & (titanic_df["Age"].isNull()), 33).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Mrs") & (titanic_df["Age"].isNull()), 36).otherwise(titanic_df["Age"]))

# COMMAND ----------

#Check the imputation
titanic_df.filter(titanic_df.Age==46).select("Initial").show()

titanic_df.select("Age").show()

# COMMAND ----------

# Embarked feature has only two missining values. Let's check values within Embarked
titanic_df.groupBy("Embarked").count().show()

# COMMAND ----------

# Majority Passengers boarded from "S". We can impute with "S"
titanic_df = titanic_df.na.fill({"Embarked" : 'S'})


# COMMAND ----------

# We can drop Cabin features as it has lots of null values
titanic_df = titanic_df.drop("Cabin")
titanic_df.printSchema()

# COMMAND ----------

#We can create a new feature called "Family_size" and "Alone" and analyse it. This feature is the summation of Parch(parents/children) and SibSp(siblings/spouses). It gives us a combined data so that we can check if survival rate have anything to do with family size of the passengers

titanic_df = titanic_df.withColumn("Family_Size",col('SibSp')+col('Parch'))
titanic_df.groupBy("Family_Size").count().show()
titanic_df = titanic_df.withColumn('Alone',lit(0))

titanic_df = titanic_df.withColumn("Alone",when(titanic_df["Family_Size"] == 0, 1).otherwise(titanic_df["Alone"]))
titanic_df.columns

# COMMAND ----------

# Lets convert Sex, Embarked & Initial columns from string to number using StringIndexer

indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(titanic_df) for column in ["Sex","Embarked","Initial"]]
pipeline = Pipeline(stages=indexers)
titanic_df = pipeline.fit(titanic_df).transform(titanic_df)

# COMMAND ----------

titanic_df.show()
titanic_df.printSchema()

# COMMAND ----------

#Drop columns which are not required
titanic_df = titanic_df.drop("PassengerId","Name","Ticket","Cabin","Embarked","Sex","Initial")
titanic_df.show()

# COMMAND ----------

# Let's put all features into vector

feature = VectorAssembler(inputCols=titanic_df.columns[1:],outputCol="features")
feature_vector= feature.transform(titanic_df)
feature_vector.show()

# COMMAND ----------

# Now that the data is all set, let's split it into training and test. I'll be using 80% of it.
(trainingData, testData) = feature_vector.randomSplit([0.8, 0.2],seed = 11)
