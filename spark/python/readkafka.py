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

    # Count the number of distinct MAC addresses
    # distinct_macs = df.agg(approx_count_distinct("mac").alias("distinct_macs"))
    # distinct_macs = df.groupBy(window("timestamp", "1 minute")).agg(approx_count_distinct("mac").alias("distinct_macs"))
    distinct_macs = df.withWatermark("timestamp", "30 seconds") \
                    .groupBy(window("timestamp", "1 minute", "30 seconds")) \
                    .agg(approx_count_distinct("mac").alias("distinct_macs"))

    # add column approx_people considering each person has 3 devices
    distinct_macs = distinct_macs.withColumn("approx_people", col("distinct_macs") / 3)
    # order by window start
    # distinct_macs = distinct_macs.orderBy("window.start", ascending=False)
    # using sort instead of orderBy
    # distinct_macs = distinct_macs.sort("window.start", ascending=False)

    # same query but output to elasticsearch
    distinct_macs \
        .writeStream \
        .format("es") \
        .option("checkpointLocation", "checkpoints") \
        .start(INDEX)
    
    # Start the streaming query
    query = distinct_macs \
        .orderBy("window.start", ascending=False) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    # query2 = df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()