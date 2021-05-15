from pyspark.sql import SparkSession


if __name__=="__main__":
    spark=SparkSession.builder.appName("Raw Queries").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    # Load is a generic way of loading files
    df=spark.read.load("../datasets/stu-list.jsonl",format='json')
    df.printSchema()


    # Saving into persitent storage (Hive) inside a spark-warehouse
    # df.write.saveAsTable("student_list_jsonldfdf")

    # load the given table 
    # spark.sql("select * from student_list_jsonldfdf").show(3)    

    # Save partitions: partition by some column
    df.write.partitionBy("Club").saveAsTable("student_partitionedby_club3")

    # Read partition
    spark.sql("show partitions student_partitionedby_club3").show()

    #Read data from each partition
    spark.sql("select Name, ID from student_partitionedby_club3 where Club='0'").show()

    spark.stop()