'''
This program shows creation of a simple spark session
'''

from pyspark.sql import SparkSession


if __name__=="__main__":
    # Create a spark session object with appname and master
    spark=SparkSession.builder.master("local[4]").appName("simple spark session").getOrCreate()
    
    # Only log error messages
    spark.sparkContext.setLogLevel("ERROR")

    # Show spark appname
    print(spark.sparkContext.appName)

    #stop the session to deallocate resources
    spark.stop()

