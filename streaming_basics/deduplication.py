from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType


# Ignoring duplicates


if __name__=="__main__":
    spark= SparkSession.builder.appName("deduplication").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Here we define a guid to uniqely identify a record
    recordSchema=StructType().add("guid","string").add("timestamp","timestamp").add("val", "integer")

    streamDF=spark.readStream.schema(recordSchema).option("sep","\t").csv("../data/streaming-data/event-time-data")

    print(streamDF.isStreaming)

    # Watermark tracks in time and waits for late data for given time
    streamDF.withWatermark("timestamp","20 seconds")

    # Drop duplicates. withWatermark is used here to limit how late 
    # the duplicate data can be and system will accordingly limit the state
    streamDF.dropDuplicates()

    windowDF=streamDF.groupBy(
        window("timestamp","5 seconds")
    ).count()
    
    query=windowDF.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

    spark.stop()

