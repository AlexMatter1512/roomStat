
    ########################################################################################################################
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