from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler

# Create a SparkSession
spark = SparkSession.builder.appName("LogisticRegressionPrediction").getOrCreate()

# Load the data from "random_data.csv"
data = spark.read.csv("training.csv", header=True, inferSchema=True)
# data = data.drop("comfortable")

# Assemble the features into a single vector
assembler = VectorAssembler(inputCols=["temperature", "humidity", "light"], outputCol="features")
data = assembler.transform(data)

# Load the logistic regression model
model = LogisticRegressionModel.load("./logistic_regression_model")

# Make predictions on the data
predictions = model.transform(data)

# print false positives, false negatives, true positives, and true negatives
predictions.groupBy("comfortable", "prediction").count().show()

# Output the predictions to the console (top 500 rows only)
predictions.show(100)

# Stop the SparkSession
spark.stop()