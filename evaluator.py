from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Logistic Regression Example") \
        .getOrCreate()
    
    # Load CSV data
    data = spark.read.csv('training.csv', header=True, inferSchema=True)
    
    # Assemble features into a single vector
    assembler = VectorAssembler(
        inputCols=['light', 'temperature', 'humidity'],
        outputCol='features'
    )
    assembled_data = assembler.transform(data).select(col('features'), col('comfortable').alias('label'))
    
    # Split data into training (70%) and test (30%) sets
    train_data, test_data = assembled_data.randomSplit([0.7, 0.3], seed=42)
    
    # Create and train logistic regression model
    lr = LogisticRegression(featuresCol='features', labelCol='label')
    lr_model = lr.fit(train_data)
    
    # Evaluate model on test data
    test_results = lr_model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='label')
    accuracy = evaluator.evaluate(test_results)
    
    # print the true positives, false positives, true negatives, and false negatives
    test_results.groupBy('label', 'prediction').count().show()
    #print the total number of records in the test data
    print(f"Total records: {test_results.count()}")

    print(f"Model accuracy: {accuracy}")

    #save the model
    lr_model.save("./logistic_regression_model")
    

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
