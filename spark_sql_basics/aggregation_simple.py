from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min
from pyspark.sql.types import IntegerType

if __name__=="__main__":
    
    spark=SparkSession.builder.appName("aggreagating spark dataframe").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df=spark.read.json("../datasets/stu-list.jsonl")
    df.printSchema()

    # The dataframe obtained above may not have proper column names, types etc.
    # we can use cols().alias() to give alias to the dataframes
    # cols.cast() casts the column to another type
    students=df.select(col('Persona').alias('Ratings').cast("int"),col('ID'),
                        col('Name'),col('Club'), col('Class').cast("int"))

    students.printSchema()

    # calling aggregate functions on dataFrame
    students.select(avg("Ratings")).show()
    students.select(max("Class")).show()
    
    spark.stop()

