// Databricks notebook source
// MAGIC %md ## Advanced Analytics with Spark Ch 1
// MAGIC #### Files loaded from U of Irvine Machine Learning Repo <a href="https://archive.ics.uci.edu/ml/datasets.html">Link </a>
// MAGIC #### Files sourced from Record Linkage Comparison Patterns Data Set <a href="http://bit.ly/1Aoywaq">donation.zip</a> Page: <a href="https://archive.ics.uci.edu/ml/datasets/record+linkage+comparison+patterns">Link</a>

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/linkage/"))

// COMMAND ----------

val rawblocks = sc.textFile("dbfs:/linkage")
// rawblocks.first
var head = rawblocks.take(10)

// COMMAND ----------

// MAGIC %md #### isHeader function to gleen out headers from data

// COMMAND ----------

def isHeader(line: String): Boolean = {
  line.contains("id_1")
}

// COMMAND ----------

// MAGIC %md #### testing header 

// COMMAND ----------

head.filter(isHeader).foreach(println)

head.filter(x => !isHeader(x)).length