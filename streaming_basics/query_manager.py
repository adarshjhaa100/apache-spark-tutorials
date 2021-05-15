from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, IntegerType, StructType



if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Create a schema for reading csv record
    recordSchema=StructType().add("uid","string").add("timestamp","timestamp").add("val","integer")

    streamDF=spark.readStream.option("sep","\t").schema(recordSchema).csv("../data/streaming-data/event-time-data")

    print(streamDF.isStreaming)

    streamDF.printSchema()

    # create a window DF
    # Bucketize rows into one or more time windows given a timestamp specifying column.
    windowDF=streamDF.groupBy(
        window("timestamp","5 seconds")
    ).sum("val")



    '''
    Checkpoint data: 
    Stores checkpoint for operations in writeStream
    the metadata contains the query ID 
    '''

    query=windowDF.writeStream.outputMode("update").format("console").option("checkpointLocation","check-data1").start()


    # Active streams
    print(len(spark.streams.active))

    # Here, we have only one query above for writing to console
    for q in spark.streams.active:
        print(q.id)

    # Obtaining query info for current spark session    
    print(spark.streams.get(query.id).runId)
    print(spark.streams.get(query.id).lastProgress)
    print(spark.streams.get(query.id).status)


    # query.awaitTermination()
    spark.streams.awaitAnyTermination()

    spark.stop()