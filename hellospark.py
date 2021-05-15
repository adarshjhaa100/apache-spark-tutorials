"""SimpleApp.py"""
from pyspark.sql import SparkSession
from pyspark.context import SparkContext


logFile = "../datasets/james-joyce-ulysses.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").master("local").getOrCreate()
spark.sparkContext.setLogLevel("OFF")


rolls=[1,2,3,4]

students=spark.sparkContext.parallelize()

spark.stop()
