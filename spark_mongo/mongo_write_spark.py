from pyspark.sql import SparkSession


if __name__=="__main__":

    # Set input uri, output uri and spark jars package
    spark=SparkSession.builder.appName("mongo simple app").master("local[*]")\
        .config("spark.mongodb.input.uri","mongodb://127.0.0.1:27017/University.Teachers")\
        .config("spark.mongodb.output.uri","mongodb://127.0.0.1:27017/University.Teachers")\
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").getOrCreate()  

    spark.sparkContext.setLogLevel("ERROR")

    df=spark.read.format("mongo").load()

    df.printSchema()

    # Write using spark dataframe
    # The method creates dataframe from an RDD, row or Pandas dataframe
    teachers=spark.createDataFrame([("Slayer","ert"),("player","22")])

    # save the document to spark: Here, by default, 
    # data is written to where we have specified spark.mongodb.output.uri
    teachers.write.format("mongo").mode("append").save()


    spark.stop()  