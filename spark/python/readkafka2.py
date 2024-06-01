from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, approx_count_distinct, col, current_timestamp, expr
from pyspark.sql.functions import from_json
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
    spark = SparkSession.builder \
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
    
    # Define schema
    schema = StructType() \
        .add("mac", "string") \
        .add("rssi", "integer") \
        .add("timestamp", "double")
    
    # Extract and parse JSON data
    df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
    df = df.select("parsed_value.*")

    # Convert the timestamp from double to timestamp type
    #df = df.withColumn("@timestamp", (col("@timestamp")).cast("timestamp"))
    
    # Filter data to include only the last 30 seconds
    df_filtered = df.withColumn("current_time", current_timestamp())
    # cast the current_time to double to match the timestamp
    df_filtered = df_filtered.withColumn("current_time", (col("current_time")).cast("double"))
    df_filtered = df_filtered.withColumn("time_diff", expr("current_time - timestamp"))
    # df_filtered = df_filtered.filter(col("time_diff") <= 30)
    # df_filtered = df_filtered.withColumn("time_diff", date_diff(col("current_time"), col("@timestamp")))
    # df_filtered = df_filtered.filter(col("time_diff") <= 30)
    
    # Count the number of distinct MAC addresses
    distinct_macs = df_filtered.agg(approx_count_distinct("mac").alias("distinct_macs"))

    # Add column approx_people considering each person has 3 devices
    distinct_macs = distinct_macs.withColumn("approx_people", col("distinct_macs") / 3)

    # Start the streaming query
    query = distinct_macs \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    # Optionally, you can also start another query to see the raw data being filtered
    query2 = df_filtered \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    # Wait for the queries to terminate
    query.awaitTermination()
    query2.awaitTermination()

if __name__ == "__main__":
    main()
