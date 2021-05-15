from pyspark.sql import SparkSession


if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df=spark.read.json("../datasets/stu-list.jsonl")
    df.printSchema()
    
    # For running sql queries we need a view
    df.createOrReplaceTempView("students")

    # run sql query within spark.sql() and print it with .show()
    spark.sql("select ID,Name,Info from students").show()

    spark.sql("select ID,Name from students order by Seat desc limit 5").show()





    spark.stop()

    
