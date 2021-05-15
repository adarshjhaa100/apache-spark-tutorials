from pyspark.sql import SparkSession


if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    # Load is a generic way of loading files
    df=spark.read.load("../datasets/stu-list.jsonl",format='json')
    df.printSchema()

    # Save a file as parquet(A format by apache for better compression and encoding)
    # df.select(df['ID'],df['Name'],df["Info"]).write.save("data/stu-list.parquet", 
    # format="parquet")

    #  Another way of writing
    df.write.csv("data/stu-list.csv")

    # Modes for saving a file: append, error(gives an error if exists), ignore, overrite
    df.write.mode("append").json("data/stu-mode-list.jsonl")

   


    spark.stop()