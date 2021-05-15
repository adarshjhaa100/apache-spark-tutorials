from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split



if __name__=="__main__":
    spark=SparkSession.builder.master("local[*]").appName("Streaming Query").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    input=spark.readStream.format("socket").option("host","localhost").option("port",8088).load()

    print(input.isStreaming)


    wordsDF=input.select(explode(split(input.value," ")).alias("Words"))

    query=wordsDF.writeStream.outputMode("append").format("console").start()


    print(len(spark.streams.active))

    # query details
    print(query.id)  # Returns the unique id of this query that persists across restarts from checkpoint data
    print(query.runId) # local ID
    print(query.lastProgress) # returns the most recent StreamingQueryProgress update of this streaming query
    print(query.status) # current status of query

    query.awaitTermination()
    spark.stop()




