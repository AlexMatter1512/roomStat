from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import countDistinct, approx_count_distinct
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType

import os

BROKER = os.environ.get('BROKER', 'broker:9092')
TOPIC = os.environ.get('TOPIC', 'macs')
FallbackBroker = 'broker:9092'
FallbackTopic = 'macs'

if not BROKER:
    print('No broker specified, using default:', FallbackBroker)
    BROKER = FallbackBroker
if not TOPIC:
    print('No topic specified, using default:', FallbackTopic)
    TOPIC = FallbackTopic

def main():
    # Create a SparkSession
    spark: SparkSession = SparkSession.builder \
        .appName("KafkaReader") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Read data from Kafka topic as a DataFrame
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", TOPIC) \
        .load()
    
    # the default schema of kafka message is key, value and other stuff, but the entire json message is in the "value" column
    # Define our custom schema of the data which has to match the json message (column whose names not match will have null data)
    schema = StructType() \
        .add("mac", "string") \
        .add("rssi", "integer") \
        .add("timestamp", "double")
    
    # Extract the value column from the Kafka message and parse it in JSON format
    # this will create a dataframe with a single column named "parsed_value" which contains the parsed json message
    df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
    df.printSchema()
    # this will create a dataframe with columns "mac", "rssi" and "@timestamp" which are the keys of the json message
    df = df.select("parsed_value.*")
    df.printSchema()

    

    # Count the number of distinct MAC addresses
    distinct_macs = df.agg(approx_count_distinct("mac").alias("distinct_macs"))

    # add column approx_people considering each person has 3 devices
    distinct_macs = distinct_macs.withColumn("approx_people", col("distinct_macs") / 3)

    # Start the streaming query
    query = distinct_macs \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    query2 = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    # Wait for the query to terminate
    query.awaitTermination()
    query2.awaitTermination()

if __name__ == "__main__":
    main()