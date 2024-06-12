import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, approx_count_distinct, window, date_format, unix_timestamp
from pyspark.sql.types import StructType, TimestampType
from pyspark.conf import SparkConf
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# KAFKA
BROKER = os.environ.get('BROKER', 'broker:9092')
TOPIC = os.environ.get('TOPIC', 'room')
# ELASTICSEARCH
ELASTIC_HOST = os.environ.get('ELASTIC', 'elasticsearch')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')
INDEX = os.environ.get('INDEX', 'room')

#SparkSession
sparkConf = SparkConf().set("es.nodes", ELASTIC_HOST).set("es.port", ELASTIC_PORT)
spark: SparkSession = SparkSession.builder \
    .appName("roomStat") \
    .config(conf=sparkConf) \
    .getOrCreate()
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

print(f"Model accuracy: {accuracy}")

def main():
    #Lower the log level
    spark.sparkContext.setLogLevel("WARN")

    # Room dataframe
    roomDf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", TOPIC) \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Define our custom schema of the data which has to match the json message (column whose names not match will have null data)
    roomDfSchema = StructType() \
        .add("mac", "string") \
        .add("rssi", "integer") \
        .add("temperature", "double") \
        .add("humidity", "double") \
        .add("light", "double") \
        .add("timestamp", "double")
    
    # The default schema of kafka message is key, value and other stuff, but the entire json message is in the "value" column
    # Extract the value column from the Kafka message and parse it in JSON format
    # this will create a dataframe with a single column named "parsed_value" which contains the parsed json message
    roomDf = roomDf.select(from_json(col("value").cast("string"), roomDfSchema).alias("parsed_value"))

    roomDf.printSchema()

    # this will create a dataframe with columns "mac", "rssi", "temperature", "humidity", "light" and "@timestamp" which are the keys of the json message
    roomDf = roomDf.select("parsed_value.*")
    roomDf.printSchema()

    # Cast the timestamp to a timestamp type
    roomDf = roomDf.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # convert the timestamp to a string in the format "yyyy-MM-dd'T'HH:mm:ss.SSSZ" for elasticsearch compatibility
    # roomDfEsTs = roomDf.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    
    # adding column suggestion using the logistic regression model, this is a binary classification model that predicts if the room is comfortable or not
    # the model is trained using the number of devices in the room, the temperature, the humidity and the light
    # import the logistric regression model
    # lr = LogisticRegressionModel.load("./logistic_regression_model")

    # # Assemble features into a single vector
    # assembler = VectorAssembler(inputCols=["temperature", "humidity", "light"], outputCol="features")
    roomDf = assembler.transform(roomDf) # add the features column to the dataframe

    # # Predict the suggestion
    roomDf = lr_model.transform(roomDf) # add the prediction column to the dataframe

    # add the prediction_str column that maps 0 to comfortable and 1 to not comfortable
    roomDf = roomDf.withColumn("prediction_str", when(col("prediction") == 1, "comfortable").otherwise("not comfortable"))

    roomDf = roomDf.select("mac", "rssi", "temperature", "humidity", "light", "prediction", "prediction_str", "timestamp")
    # roomDf.printSchema()

    # Write the dataframe to elasticsearch
    # query = roomDfEsTs \
    #     .writeStream \
    #     .format("es") \
    #     .option("checkpointLocation", "./checkpoint") \
    #     .start(INDEX)
    
    # write the dataframe to the console
    # query = roomDf \
    #     .writeStream \
    #     .foreachBatch(foreach_batch_logistic_regression) \
    #     .start()

    query = roomDf \
        .writeStream \
        .format("console") \
        .start()
    
    # elasticsearch query
    roomDf \
        .writeStream \
        .format("es") \
        .option("checkpointLocation", "./checkpoint") \
        .start(INDEX)
    
    query.awaitTermination()

if __name__ == "__main__":
    main()