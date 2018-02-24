# Databricks notebook source
# MAGIC %md # Let's use the Databricks Platform to predict Diabetes
# MAGIC <img src="https://bmathewtest.blob.core.windows.net/test/diabetes.png"   width="700" height="300"> 

# COMMAND ----------

# MAGIC %md ## Explore the source data
# MAGIC - Diabetic and non-diabetic female patients
# MAGIC - 'diabetes' column indicates if the patient is diabetic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.diabetes_data_export_csv

# COMMAND ----------

# MAGIC %md ##Visualize the data

# COMMAND ----------

# MAGIC %md ### Distribution of non-diabetic/diabetic across age

# COMMAND ----------

# MAGIC %sql
# MAGIC select age, diabetes from diabetes_data_export_csv sort by age

# COMMAND ----------

# MAGIC %md ### Geographic distribution of diabetic patients

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT state, count(1) AS diabetic_count FROM diabetes_data_export_csv WHERE diabetes = 1 GROUP BY state 

# COMMAND ----------

# MAGIC %md ### Relationship between Blood Pressure and BMI
# MAGIC - Visualize the data to see if there is some correlation
# MAGIC - Scatter plot below indicates that there is no correlation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diabetes_data_export_csv sort by age

# COMMAND ----------

# MAGIC %md ### Top 10 affected age groups with Diabetes

# COMMAND ----------

# MAGIC %sql
# MAGIC select age, count(1) as diabetic_count from diabetes_data_export_csv where diabetes = 1 group by age order by count(1) desc limit 10

# COMMAND ----------

# MAGIC %md ## Let's build a machine learning model to predict dieabetes using the data

# COMMAND ----------

# MAGIC %md ### 1. Preprocess the data
# MAGIC - Transform features into a vector
# MAGIC - Create a Machine Learning pipeline
# MAGIC - Split data into Training and Test sets

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler

#source = spark.table("diabetes_data")
#source.createTempTable("")
sql = "SELECT patient_id, plasma_glucose, blood_pressure, triceps_skin_thickness, insulin, bmi, diabetes_pedigree, age, diabetes as label from diabetes_data_export_csv"
dataset = spark.sql(sql)
cols = dataset.columns
stages = [] 

# Transform all features into a vector using VectorAssembler
numericCols = ["plasma_glucose", "blood_pressure", "triceps_skin_thickness", "insulin", "bmi", "diabetes_pedigree", "age"]
assemblerInputs = numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# Create a Pipeline.
pipeline = Pipeline(stages=stages)
# Run the feature transformations.
pipelineModel = pipeline.fit(dataset)
dataset = pipelineModel.transform(dataset)

# Keep relevant columns
selectedcols = ["features"] + cols
dataset = dataset.select(selectedcols)
display(dataset)

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.6, 0.4], seed = 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build a Logistic Regression model using the training data

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# Create initial LogisticRegression model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)

# Train model with Training Data
lrModel = lr.fit(trainingData)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run test data through model

# COMMAND ----------

# Make predictions on test data using the transform() method.
# LogisticRegression.transform() will only use the 'features' column.
predictions = lrModel.transform(testData)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Model
# MAGIC - Multiple metrics to evaluate model accurary
# MAGIC - Area Under ROC is used below

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Evaluate model
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expose model output in a structured format so that anyone can easily query the model results

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import expr
from pyspark.sql.functions import desc
from pyspark.sql.functions import col

def probability(v):
  return float(v.array[1])
p1 = udf(probability, DoubleType())

selected = predictions.select("patient_id", "plasma_glucose", "blood_pressure", "triceps_skin_thickness", "insulin", "bmi", "diabetes_pedigree", "age", col("label").alias("diabetes"),(p1("probability")).alias("probability_diabetic"),(1-p1("probability")).alias("probability_non_diabetic"),"prediction")

selected.registerTempTable("diabetes_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL query against the model output
# MAGIC - The column 'diabetes' is the label column 
# MAGIC - The column 'prediction' is what we predicted 
# MAGIC - Sorting the data based on the prediction column

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diabetes_analysis ORDER BY prediction DESC