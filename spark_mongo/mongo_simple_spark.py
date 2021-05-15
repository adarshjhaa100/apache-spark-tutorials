from pyspark.sql import SparkSession


if __name__=="__main__":

    # Set input uri, output uri and spark jars package
    spark=SparkSession.builder.appName("mongo simple app").master("local[*]")\
        .config("spark.mongodb.input.uri","mongodb://127.0.0.1:27017/University.Students")\
        .config("spark.mongodb.output.uri","mongodb://127.0.0.1:27017/University.Students")\
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").getOrCreate()  

    spark.sparkContext.setLogLevel("ERROR")


    df=spark.read.format("mongo").load()

    df.printSchema()



    spark.stop()  