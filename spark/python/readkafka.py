import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, approx_count_distinct, window
from pyspark.sql.types import StructType
from pyspark.conf import SparkConf

# KAFKA
BROKER = os.environ.get('BROKER', 'broker:9092')
TOPIC = os.environ.get('TOPIC', 'macs')
# ELASTICSEARCH
ELASTIC_HOST = os.environ.get('ELASTIC', 'elasticsearch')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')
INDEX = os.environ.get('INDEX', 'macs')

def main():

    #SparkSession
    sparkConf = SparkConf().set("es.nodes", ELASTIC_HOST).set("es.port", ELASTIC_PORT)
    spark: SparkSession = SparkSession.builder \
        .appName("roomStat") \
        .config(conf=sparkConf) \
        .getOrCreate()

    #Lower the log level
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

    # Cast the timestamp to a timestamp type
    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # Count the number of distinct MAC addresses in a 2 minute window with a 30 second slide
    distinct_macs = df.withWatermark("timestamp", "1 minute") \
                    .groupBy(window("timestamp", "2 minute", "30 seconds")) \
                    .agg(approx_count_distinct("mac").alias("distinct_macs"))

    # adding column approx_people considering each person has 3 devices
    distinct_macs = distinct_macs.withColumn("approx_people", col("distinct_macs") / 3)

    # adding the start and end of the window to the output for better readability
    distinct_macs_reduced_output = distinct_macs.select("window.start", "window.end", "distinct_macs", "approx_people")

    # elasticsearch sink
    distinct_macs \
        .writeStream \
        .format("es") \
        .option("checkpointLocation", "checkpoints") \
        .start(INDEX)#.awaitTermination()
    
    # console sink
    query = distinct_macs_reduced_output \
        .orderBy("window.start", ascending=False) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # at least one query must be 
    # print("Awaiting termination")
    # while True:
    #     pass
    query.awaitTermination()

if __name__ == "__main__":
    main()