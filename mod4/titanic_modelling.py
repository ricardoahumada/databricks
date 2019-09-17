# Databricks notebook source
# MAGIC %md ##Importing needful libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import QuantileDiscretizer

# COMMAND ----------

# MAGIC %md ##Connect to source and read data

# COMMAND ----------

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

display(titanic_df)

# COMMAND ----------

titanic_df.printSchema()

# COMMAND ----------

passengers_count = titanic_df.count()

# COMMAND ----------

print(passengers_count)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Viewing few rows

# COMMAND ----------

titanic_df.show(5)

# COMMAND ----------

# MAGIC %md Summary of data

# COMMAND ----------

titanic_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### checking Schema of our dataset

# COMMAND ----------

titanic_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### selecting few features

# COMMAND ----------

titanic_df.select("Survived","Pclass","Embarked").show()

# COMMAND ----------

# MAGIC %md ### Let's do some simple exploratory data analysis (EDA)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Knowing the number of Passengers Survived ?

# COMMAND ----------

titanic_df.groupBy("Survived").count().show()

# COMMAND ----------

gropuBy_output = titanic_df.groupBy("Survived").count()

# COMMAND ----------

display(gropuBy_output)

# COMMAND ----------

# MAGIC %md Out of 891 passengers in dataset, only about 342 survived.

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### To know the particulars about survivors we have to explore more of the data.
# MAGIC ###### The survival rate can be determined by different features of the dataset such as Sex, Port of Embarcation, Age; few to be mentioned.

# COMMAND ----------

# MAGIC %md Checking survival rate using feature Sex 

# COMMAND ----------

titanic_df.groupBy("Sex","Survived").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Although the number of males are more than females on ship, the female survivors are twice the number of males saved.

# COMMAND ----------

titanic_df.groupBy("Pclass","Survived").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Here it can be seen that the Pclass1 people were given priority to pclass3 people, even though
# MAGIC We can clearly see that Passenegers Of Pclass 1 were given a very high priority while rescue. Even though the the number of Passengers in Pclass 3 were a lot higher, still the number of survival from them is very low.

# COMMAND ----------

# MAGIC %md #### Checking Null values

# COMMAND ----------

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

# COMMAND ----------

# Calling function
null_columns_count_list = null_value_count(titanic_df)


# COMMAND ----------

spark.createDataFrame(null_columns_count_list, ['Column_With_Null_Value', 'Null_Values_Count']).show()

# COMMAND ----------

# MAGIC %md Age feature has 177 null values.

# COMMAND ----------

mean_age = titanic_df.select(mean('Age')).collect()[0][0]
print(mean_age)

# COMMAND ----------

titanic_df.select("Name").show()

# COMMAND ----------

# MAGIC %md To replace these NaN values, we can assign them the mean age of the dataset.But the problem is, there were many people with many different ages. We just cant assign a 4 year kid with the mean age that is 29 years. 

# COMMAND ----------

# MAGIC %md 
# MAGIC we can check the Name feature. Looking upon the feature, we can see that the names have a salutation like Mr or Mrs. Thus we can assign the mean values of Mr and Mrs to the respective groups

# COMMAND ----------

titanic_df = titanic_df.withColumn("Initial",regexp_extract(col("Name"),"([A-Za-z]+)\.",1))


# COMMAND ----------

# MAGIC %md 
# MAGIC Using the Regex ""[A-Za-z]+)." we extract the initials from the Name. It looks for strings which lie between A-Z or a-z and followed by a .(dot).

# COMMAND ----------

titanic_df.show()

# COMMAND ----------

titanic_df.select("Initial").distinct().show()


# COMMAND ----------

# MAGIC %md 
# MAGIC There are some misspelled Initials like Mlle or Mme that stand for Miss. I will replace them with Miss and same thing for other values.

# COMMAND ----------

titanic_df = titanic_df.replace(['Mlle','Mme', 'Ms', 'Dr','Major','Lady','Countess','Jonkheer','Col','Rev','Capt','Sir','Don'],
               ['Miss','Miss','Miss','Mr','Mr',  'Mrs',  'Mrs',  'Other',  'Other','Other','Mr','Mr','Mr'])


# COMMAND ----------

titanic_df.select("Initial").distinct().show()


# COMMAND ----------

# MAGIC %md lets check the average age by Initials

# COMMAND ----------

titanic_df.groupby('Initial').avg('Age').collect()

# COMMAND ----------

# MAGIC %md Let's impute missing values in age feature based on average age of Initials

# COMMAND ----------

titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Miss") & (titanic_df["Age"].isNull()), 22).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Other") & (titanic_df["Age"].isNull()), 46).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Master") & (titanic_df["Age"].isNull()), 5).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Mr") & (titanic_df["Age"].isNull()), 33).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Mrs") & (titanic_df["Age"].isNull()), 36).otherwise(titanic_df["Age"]))


# COMMAND ----------

# MAGIC %md
# MAGIC Check the imputation 

# COMMAND ----------

titanic_df.filter(titanic_df.Age==46).select("Initial").show()


# COMMAND ----------

titanic_df.select("Age").show()

# COMMAND ----------

# MAGIC %md Embarked feature has only two missining values. Let's check values within Embarked

# COMMAND ----------

titanic_df.groupBy("Embarked").count().show()

# COMMAND ----------

# MAGIC %md Majority Passengers boarded from "S". We can impute with "S"

# COMMAND ----------

titanic_df = titanic_df.na.fill({"Embarked" : 'S'})


# COMMAND ----------

# MAGIC %md We can drop Cabin features as it has lots of null values

# COMMAND ----------

titanic_df = titanic_df.drop("Cabin")

# COMMAND ----------

titanic_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC We can create a new feature called "Family_size" and "Alone" and analyse it. This feature is the summation of Parch(parents/children) and SibSp(siblings/spouses). It gives us a combined data so that we can check if survival rate have anything to do with family size of the passengers

# COMMAND ----------

titanic_df = titanic_df.withColumn("Family_Size",col('SibSp')+col('Parch'))

# COMMAND ----------

titanic_df.groupBy("Family_Size").count().show()

# COMMAND ----------

titanic_df = titanic_df.withColumn('Alone',lit(0))


# COMMAND ----------

titanic_df = titanic_df.withColumn("Alone",when(titanic_df["Family_Size"] == 0, 1).otherwise(titanic_df["Alone"]))

# COMMAND ----------

titanic_df.columns

# COMMAND ----------

# MAGIC %md Lets convert Sex, Embarked & Initial columns from string to number using StringIndexer

# COMMAND ----------

indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(titanic_df) for column in ["Sex","Embarked","Initial"]]
pipeline = Pipeline(stages=indexers)
titanic_df = pipeline.fit(titanic_df).transform(titanic_df)

# COMMAND ----------

titanic_df.show()

# COMMAND ----------

titanic_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Drop columns which are not required

# COMMAND ----------

titanic_df = titanic_df.drop("PassengerId","Name","Ticket","Cabin","Embarked","Sex","Initial")

# COMMAND ----------

titanic_df.show()

# COMMAND ----------

# MAGIC %md Let's put all features into vector

# COMMAND ----------

feature = VectorAssembler(inputCols=titanic_df.columns[1:],outputCol="features")
feature_vector= feature.transform(titanic_df)

# COMMAND ----------

feature_vector.show()

# COMMAND ----------

# MAGIC %md Now that the data is all set, let's split it into training and test. I'll be using 80% of it.

# COMMAND ----------

(trainingData, testData) = feature_vector.randomSplit([0.8, 0.2],seed = 11)

# COMMAND ----------

# MAGIC %md ### Modelling 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###### Here is the list of few Classification Algorithms from Spark ML 
# MAGIC 
# MAGIC LogisticRegression
# MAGIC 
# MAGIC DecisionTreeClassifier
# MAGIC 
# MAGIC RandomForestClassifier
# MAGIC 
# MAGIC Gradient-boosted tree classifier
# MAGIC 
# MAGIC NaiveBayes
# MAGIC 
# MAGIC Support Vector Machine

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### LogisticRegression

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol="Survived", featuresCol="features")
#Training algo
lrModel = lr.fit(trainingData)
lr_prediction = lrModel.transform(testData)
lr_prediction.select("prediction", "Survived", "features").show()
evaluator = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluating accuracy of LogisticRegression.

# COMMAND ----------

lr_accuracy = evaluator.evaluate(lr_prediction)
print("Accuracy of LogisticRegression is = %g"% (lr_accuracy))
print("Test Error of LogisticRegression = %g " % (1.0 - lr_accuracy))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### DecisionTreeClassifier

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
dt = DecisionTreeClassifier(labelCol="Survived", featuresCol="features")
dt_model = dt.fit(trainingData)
dt_prediction = dt_model.transform(testData)
dt_prediction.select("prediction", "Survived", "features").show()


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluating accuracy of DecisionTreeClassifier.

# COMMAND ----------

dt_accuracy = evaluator.evaluate(dt_prediction)
print("Accuracy of DecisionTreeClassifier is = %g"% (dt_accuracy))
print("Test Error of DecisionTreeClassifier = %g " % (1.0 - dt_accuracy))


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### RandomForestClassifier

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
rf = DecisionTreeClassifier(labelCol="Survived", featuresCol="features")
rf_model = rf.fit(trainingData)
rf_prediction = rf_model.transform(testData)
rf_prediction.select("prediction", "Survived", "features").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluating accuracy of RandomForestClassifier.

# COMMAND ----------

rf_accuracy = evaluator.evaluate(rf_prediction)
print("Accuracy of RandomForestClassifier is = %g"% (rf_accuracy))
print("Test Error of RandomForestClassifier  = %g " % (1.0 - rf_accuracy))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Gradient-boosted tree classifier

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
gbt = GBTClassifier(labelCol="Survived", featuresCol="features",maxIter=10)
gbt_model = gbt.fit(trainingData)
gbt_prediction = gbt_model.transform(testData)
gbt_prediction.select("prediction", "Survived", "features").show()


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluate accuracy of Gradient-boosted.

# COMMAND ----------

gbt_accuracy = evaluator.evaluate(gbt_prediction)
print("Accuracy of Gradient-boosted tree classifie is = %g"% (gbt_accuracy))
print("Test Error of Gradient-boosted tree classifie %g"% (1.0 - gbt_accuracy))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluating accuracy of DecisionTreeClassifier.

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### NaiveBayes

# COMMAND ----------

from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes(labelCol="Survived", featuresCol="features")
nb_model = nb.fit(trainingData)
nb_prediction = nb_model.transform(testData)
nb_prediction.select("prediction", "Survived", "features").show()


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluating accuracy of NaiveBayes.

# COMMAND ----------

nb_accuracy = evaluator.evaluate(nb_prediction)
print("Accuracy of NaiveBayes is  = %g"% (nb_accuracy))
print("Test Error of NaiveBayes  = %g " % (1.0 - nb_accuracy))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Support Vector Machine

# COMMAND ----------

from pyspark.ml.classification import LinearSVC
svm = LinearSVC(labelCol="Survived", featuresCol="features")
svm_model = svm.fit(trainingData)
svm_prediction = svm_model.transform(testData)
svm_prediction.select("prediction", "Survived", "features").show()


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Evaluating the accuracy of Support Vector Machine.

# COMMAND ----------

svm_accuracy = evaluator.evaluate(svm_prediction)
print("Accuracy of Support Vector Machine is = %g"% (svm_accuracy))
print("Test Error of Support Vector Machine = %g " % (1.0 - svm_accuracy))

# COMMAND ----------

# MAGIC %md 
# MAGIC How to increase accuracy of a model ?
# MAGIC   * Add new features or drop existing features and train model.
# MAGIC   * Tune ML algorithm (https://spark.apache.org/docs/latest/ml-tuning.html) 

# COMMAND ----------

# MAGIC %md ### Further Reading
# MAGIC https://docs.databricks.com/spark/latest/mllib/binary-classification-mllib-pipelines.html
# MAGIC 
# MAGIC https://spark.apache.org/docs/latest/ml-classification-regression.html
