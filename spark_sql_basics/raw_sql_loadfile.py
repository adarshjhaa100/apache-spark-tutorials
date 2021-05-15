from pyspark.sql import SparkSession


if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw sql loading file").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # To load a file into a dataframe. Note: Use backticks for path
    students=spark.sql("select * from parquet.`data/stu-list.parquet`")
    students.printSchema()


    spark.stop()
