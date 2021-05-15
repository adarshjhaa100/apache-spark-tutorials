from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import TimestampType, IntegerType, StructType



if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Create a schema for reading csv record
    recordSchema=StructType().add("timestamp","timestamp").add("val","integer")

    streamDF=spark.readStream.option("sep","\t").schema(recordSchema).csv("../data/streaming-data/event-time-data")

    print(streamDF.isStreaming)

    streamDF.printSchema()

    # create a window DF
    # Bucketize rows into one or more time windows given a timestamp specifying column.
    windowDF=streamDF.groupBy(
        window("timestamp","5 seconds")
    ).sum("val")

    query=windowDF.writeStream.outputMode("update").format("console").start()

    query.awaitTermination()

    spark.stop()