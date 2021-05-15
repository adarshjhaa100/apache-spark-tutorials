from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split

'''
Inputing from spark stream
'''


if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    # text=spark.read.text("data/sample1.txt")
    

    # Reading input stream via socket 
    text=spark.readStream.format("socket").option("host","localhost").option("port",8088).load()

    
    # Explode method creates a row for each element of iterator given as input
    textDF=text.select(explode(split(text.value," ")).alias("word"))

    print(textDF)

    # For obtaining the output of stream, we need a different techinque than 
    # provided below which is covered in structured_stream_output.py
    # print(textDF.show())

    spark.stop()