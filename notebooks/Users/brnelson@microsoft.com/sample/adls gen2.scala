// Databricks notebook source
spark.conf.set("fs.azure.account.key.adbstorgen2.dfs.core.windows.net", "y7MfSulB+gYthS5dM0stOn3LoEzSMp/Q7Z1Lwlf7klOvx5sYMSWEFU1F8Iw/u2X6KDu0qqffNoRJJaEDQr5Ylw==") 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://adlsgen2@adbstorgen2.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false") 

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "8d15f4dd-3bc1-4bbd-ab06-c87876629c24")
spark.conf.set("fs.azure.account.oauth2.client.secret", "TRQUrqVd/axlMGy4lmU/Po0uXqixt3gHSR+r8+KGwDk=")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

display(dbutils.fs.ls("abfss://nifi@adbstorgen2.dfs.core.windows.net/test"))


// COMMAND ----------

// MAGIC %sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://nifi@adbstorgen2.dfs.core.windows.net/test")

// COMMAND ----------

display(dbutils.fs.ls("abfss://adlsgen2@adbstorgen2.dfs.core.windows.net/"))

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS radio_sample_data;
// MAGIC CREATE TABLE radio_sample_data
// MAGIC USING json
// MAGIC OPTIONS (
// MAGIC  path  "abfss://adlsgen2@adbstorgen2.dfs.core.windows.net/small_radio_json.json"
// MAGIC )

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT * from radio_sample_data