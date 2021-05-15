from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split

'''
Inputing from spark stream
'''


if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

   
    inputStream=spark.readStream.format("socket").option("host","localhost").option("port",8088).load()
    
    '''
    Returns True if this Dataset contains one or more sources that continuously 
    return data as it arrives. A Dataset that reads data from a streaming source 
    must be executed as a StreamingQuery using the start method in DataStreamWrite
    '''
    print(inputStream.isStreaming)

    wordsDF=inputStream.select(explode(split(inputStream.value," ")).alias("words"))
    print(wordsDF)

    # Handling the write.  
    # Writing the stream to the console using append mode(sinking to console)
    query=wordsDF.writeStream.outputMode("append").format("console").start()

    query.awaitTermination()

    spark.close()