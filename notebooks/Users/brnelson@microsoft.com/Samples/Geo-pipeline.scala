// Databricks notebook source
// MAGIC %md ### Geo Pipeline
// MAGIC Start with a DF of addresses, run addresses though LBS save new addresses to a table.

// COMMAND ----------

val df = spark.table("rawaddresses")
display(df)


// COMMAND ----------

df.count()

// COMMAND ----------

df.createOrReplaceTempView("dftab")
val addline = sqlContext.sql("SELECT CONCAT(Address, ' ',  City, ' ', State, ' ', Zip) addline FROM dftab")
display(addline)

// COMMAND ----------

