from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import split

# Create a SparkSession
spark: SparkSession = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the Kafka topic to read from
kafka_topic = "macs"

# Define the Kafka broker address
kafka_broker = "contabo_server:9092"

# Read data from Kafka topic as a DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .load()

# Extract the value column from the Kafka message
df = df.selectExpr("CAST(value AS STRING)")

# Split the value column by comma to extract MAC addresses
df = df.withColumn("mac", split(df.value, ",").getItem(0))

# Count the number of distinct MAC addresses
distinct_macs = df.groupBy("mac").count()

# Start the streaming query
query = distinct_macs \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query2

# Wait for the query to terminate
query.awaitTermination()