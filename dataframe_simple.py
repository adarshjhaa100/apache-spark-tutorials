'''
This program demonstrates a spark dataFrame(Dataset)

'''
from pyspark.sql import SparkSession


if __name__=="__main__":
    
    # Sparksession with all local threads
    spark=SparkSession.builder.appName("Simple dataframe session").master("local[*]").getOrCreate()

    # Create a dataframe to store a json dataset
    #  In pyspark ,each line should be a json object(JSON lines format)
    #  for multiline json files, set multiline=True
    df=spark.read.json("../datasets/stu-list.jsonl")
    
    # Print schema(Structure of file)
    df.printSchema()
    # df1=spark.read.json("../datasets/student-list.json", multiLine=True)

    # select some columns
    df.select(df['name'],df['class']).show()

    # # filter cols
    df.select(df['name'],df['class']).filter(df['class']>20).show()

    
    # json_string=['{"ID": "3825723", "Name": "Senpai the great", "Gender": "1", "Class": "32", "Seat": "15", "Club": "0", "Persona": "1", "Crush": "0", "BreastSize": "0", "Strength": "0", "Hairstyle": "1", "Color": "Black", "Eyes": "Black", "EyeType": "Default", "Stockings": "None", "Accessory": "0", "ScheduleTime": "7_7_8_13.01_13.375_15.5_16_17.25_99_99", "ScheduleDestination": "Spawn_Locker_Hangout_Seat_LunchSpot_Seat_Clean_Hangout_Locker_Exit", "ScheduleAction": "Stand_Stand_Read_Sit_Eat_Sit_Clean_Read_Shoes_Stand", "Info": "An average student.}',
    # '{"ID": "3825723", "Name": "Senpai the great", "Gender": "1", "Class": "32", "Seat": "15", "Club": "0", "Persona": "1", "Crush": "0", "BreastSize": "0", "Strength": "0", "Hairstyle": "1", "Color": "Black", "Eyes": "Black", "EyeType": "Default", "Stockings": "None", "Accessory": "0", "ScheduleTime": "7_7_8_13.01_13.375_15.5_16_17.25_99_99", "ScheduleDestination": "Spawn_Locker_Hangout_Seat_LunchSpot_Seat_Clean_Hangout_Locker_Exit", "ScheduleAction": "Stand_Stand_Read_Sit_Eat_Sit_Clean_Read_Shoes_Stand", "Info": "An average student.}']


    # # Create an RDD from json string
    # jsonRDD=spark.sparkContext.parallelize(json_string)
    # newStudent=spark.read.json(jsonRDD)
    # newStudent.show()
    
    # #To add more students to the dataFrame, we can use the union property
    # df=df.union(newStudent)

    df.show(120)

    spark.stop()