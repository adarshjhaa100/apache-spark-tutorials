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

    streamDF.withWatermark("timestamp","5 seconds")
    
    windowDF=streamDF.withWatermark("timestamp", "1 minutes").groupBy(
        window("timestamp","5 seconds")
    ).count()

    # output data to a file sink. Here, we have written to parquet file
    # Allowed modes: parquet, kafka, memory etc.
    # query=windowDF.writeStream.format("parquet").option("path","../data/streaming-data/file-sink1").option("checkpointLocation","checkpoint-data1").start()

    query=windowDF \
    .writeStream \
    .queryName("aggregates") \
    .outputMode("complete") \
    .format("memory") \
    .start()

    spark.sql("select * from aggregates").show()
    
    query.awaitTermination()

    spark.stop()


    