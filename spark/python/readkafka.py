import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, approx_count_distinct, window, date_format, unix_timestamp
from pyspark.sql.types import StructType, TimestampType
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
    spark.sparkContext.setLogLevel("DEBUG")
    
    # Read data from Kafka topic as a DataFrame
    # df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", BROKER) \
    #     .option("subscribe", TOPIC) \
    #     .option("failOnDataLoss", "false") \
    #     .load()
    
    # Env dataframe
    envDf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKER) \
        .option("subscribe", "envData") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # the default schema of kafka message is key, value and other stuff, but the entire json message is in the "value" column
    # Define our custom schema of the data which has to match the json message (column whose names not match will have null data)
    # schema = StructType() \
    #     .add("mac", "string") \
    #     .add("rssi", "integer") \
    #     .add("timestamp", "double")
    
    envSchema = StructType() \
        .add("light", "double") \
        .add("temperature", "double") \
        .add("humidity", "double") \
        .add("timestamp", "double")
    
    # Extract the value column from the Kafka message and parse it in JSON format
    # this will create a dataframe with a single column named "parsed_value" which contains the parsed json message
    # df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
    envDf = envDf.select(from_json(col("value").cast("string"), envSchema).alias("parsed_value"))

    # df.printSchema()
    envDf.printSchema()

    # this will create a dataframe with columns "mac", "rssi" and "@timestamp" which are the keys of the json message
    # df = df.select("parsed_value.*")
    # df.printSchema()

    envDf = envDf.select("parsed_value.*")
    envDf.printSchema()

    # Cast the timestamp to a timestamp type
    # df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    envDf = envDf.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # Cast timestamp to a string in the format "yyyy-MM-dd HH:mm:ss"
    # df1 = df.withColumn("timestamp", col("timestamp").cast("string"))

    # convert the timestamp to a string in the format "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    # df2 = df.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    envDf = envDf.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

    # df2\
    #     .writeStream \
    #     .format("es") \
    #     .option("checkpointLocation", "checkpoints") \
    #     .start(INDEX + "_raw")
    
    envDf\
        .writeStream \
        .format("es") \
        .option("checkpointLocation", "checkpoints") \
        .start("envdata")
    
    # df2\
    #     .writeStream \
    #     .format("console") \
    #     .start().awaitTermination()
    
    envDf\
        .writeStream \
        .format("console") \
        .start().awaitTermination()

if __name__ == "__main__":
    main()