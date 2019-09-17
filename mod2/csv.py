# Databricks notebook source
#Mount datalake
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

csv_path = "/mnt/data-lake/8_titanic.csv"
titanic_df = spark.read.csv(csv_path,header = 'True',inferSchema='True')

# COMMAND ----------

display(titanic_df)

# COMMAND ----------

titanic_df.printSchema()

# COMMAND ----------

passengers_count = titanic_df.count()
print(passengers_count)

# COMMAND ----------

titanic_df.show(5)

# COMMAND ----------

#Summary of data
titanic_df.describe().show()

# COMMAND ----------

#selecting few features
titanic_df.select("Survived","Pclass","Embarked").show()

# COMMAND ----------

# MAGIC %md #Simple exploratory data analysis (EDA)

# COMMAND ----------

# Knowing the number of Passengers Survived ?
titanic_df.groupBy("Survived").count().show()
gropuBy_output = titanic_df.groupBy("Survived").count()
display(gropuBy_output)

# COMMAND ----------

# Checking survival rate using feature Sex
titanic_df.groupBy("Sex","Survived").count().show()

# COMMAND ----------

# Although the number of males are more than females on ship, the female survivors are twice the number of males saved.
titanic_df.groupBy("Pclass","Survived").count().show()

# COMMAND ----------

# Import needed functions
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit

# COMMAND ----------

# MAGIC %md ##Checking Null values

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

# Calling function
null_columns_count_list = null_value_count(titanic_df)

# Creating df
spark.createDataFrame(null_columns_count_list, ['Column_With_Null_Value', 'Null_Values_Count']).show()

# COMMAND ----------

mean_age = titanic_df.select(mean('Age')).collect()[0][0]
print(mean_age)
titanic_df.select("Name").show()

# COMMAND ----------

# Replace NaN values
titanic_df = titanic_df.withColumn("Initial",regexp_extract(col("Name"),"([A-Za-z]+)\.",1))

# COMMAND ----------

titanic_df.show()

# COMMAND ----------

titanic_df.select("Initial").distinct().show()

# COMMAND ----------

# There are some misspelled Initials like Mlle or Mme that stand for Miss. I will replace them with Miss and same thing for other values.

titanic_df = titanic_df.replace(['Mlle','Mme', 'Ms', 'Dr','Major','Lady','Countess','Jonkheer','Col','Rev','Capt','Sir','Don'],
               ['Miss','Miss','Miss','Mr','Mr',  'Mrs',  'Mrs',  'Other',  'Other','Other','Mr','Mr','Mr'])

# COMMAND ----------

titanic_df.select("Initial").distinct().show()

# COMMAND ----------

# Avrg age by initials
titanic_df.groupby('Initial').avg('Age').collect()

# COMMAND ----------

# Let's impute missing values in age feature based on average age of Initials

titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Miss") & (titanic_df["Age"].isNull()), 22).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Other") & (titanic_df["Age"].isNull()), 46).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Master") & (titanic_df["Age"].isNull()), 5).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Mr") & (titanic_df["Age"].isNull()), 33).otherwise(titanic_df["Age"]))
titanic_df = titanic_df.withColumn("Age",when((titanic_df["Initial"] == "Mrs") & (titanic_df["Age"].isNull()), 36).otherwise(titanic_df["Age"]))

# COMMAND ----------

titanic_df.filter(titanic_df.Age==46).select("Initial").show()

# COMMAND ----------

titanic_df.select("Age").show()

# COMMAND ----------

# MAGIC %md ## create new feature

# COMMAND ----------

# We can create a new feature called "Family_size" and "Alone" and analyse it. This feature is the summation of Parch(parents/children) and SibSp(siblings/spouses). It gives us a combined data so that we can check if survival rate have anything to do with family size of the passengers
titanic_df = titanic_df.withColumn("Family_Size",col('SibSp')+col('Parch'))
titanic_df.groupBy("Family_Size").count().show()

# COMMAND ----------

titanic_df = titanic_df.withColumn('Alone',lit(0))

# COMMAND ----------

titanic_df = titanic_df.withColumn("Alone",when(titanic_df["Family_Size"] == 0, 1).otherwise(titanic_df["Alone"]))

# COMMAND ----------

titanic_df.columns

# COMMAND ----------

#Lets convert Sex, Embarked & Initial columns from string to number using StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(titanic_df) for column in ["Sex","Embarked","Initial"]]
pipeline = Pipeline(stages=indexers)
titanic_df = pipeline.fit(titanic_df).transform(titanic_df)

titanic_df.show()

# COMMAND ----------

titanic_df.printSchema()

# COMMAND ----------

#Drop columns which are not required
titanic_df = titanic_df.drop("PassengerId","Name","Ticket","Cabin","Embarked","Sex","Initial")
titanic_df.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# Let's put all features into vector

feature = VectorAssembler(inputCols=titanic_df.columns[1:],outputCol="features")
feature_vector= feature.transform(titanic_df)

feature_vector.show()

# COMMAND ----------

#dbutils.fs.unmount("/data-lake") 
