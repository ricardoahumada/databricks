// Databricks notebook source
// MAGIC %scala
// MAGIC // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
// MAGIC // Class.forName("org.mariadb.jdbc.Driver")
// MAGIC Class.forName("com.mysql.jdbc.Driver")

// COMMAND ----------

val jdbcHostname = "mysql-titanic.mysql.database.azure.com"
val jdbcPort = 3306
val jdbcDatabase = "titanic"

val jdbcUsername="ricardo@mysql-titanic"
val jdbcPassword="Formacion2019"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

import java.sql.DriverManager
val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
connection.isClosed()

// COMMAND ----------

val titanic_df = spark.read.jdbc(jdbcUrl, "data", connectionProperties)

// COMMAND ----------

titanic_df.show()

// COMMAND ----------

titanic_df.printSchema

// COMMAND ----------

display(titanic_df.select("Survived", "Sex"))

// COMMAND ----------

val pushdown_query = "(select * from data where Survived = true) emp_alias"
val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

// COMMAND ----------

spark.read.jdbc(jdbcUrl, "data", connectionProperties).explain(true)

// COMMAND ----------

spark.read.jdbc(jdbcUrl, "data", connectionProperties).select("Sex", "Survived", "Pclass").explain(true)

// COMMAND ----------

spark.read.jdbc(jdbcUrl, "data", connectionProperties).select("Sex", "Survived", "Pclass").where("Sex='Male'").explain(true)

// COMMAND ----------

connection.close()
