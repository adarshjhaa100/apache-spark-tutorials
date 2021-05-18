from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import isnan, when, count, col, lit
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder


# Replace column values
def replace(column, value):
    return when(column != value, column).otherwise(lit(None))



# Function to remove null values
def data_cleaning(data):
    print("data_cleaning")
    data = data.withColumn("Market Category", replace(col("Market Category"), "N/A"))
    data.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data.columns]).show()
    data = data.drop("Market Category")
    data = data.na.drop()
    print((data.count(), len(data.columns)))
    return data



# create randomforest pipeline
def create_random_pipeline():
    assembler = VectorAssembler(inputCols=["Year",
                                      "Engine HP",
                                      "Engine Cylinders",
                                      "Number of Doors",
                                      "highway MPG",
                                      "city mpg",
                                      "Popularity"], outputCol = "Attributes")

    regressor = RandomForestRegressor(featuresCol = "Attributes", labelCol="MSRP")
    pipeline = Pipeline(stages=[assembler, regressor])
    
    pipelineStr="pipeline"
    pipeline.write().overwrite().save(pipelineStr) #  Save pipeline 
    return (pipelineStr,regressor)



# Creating cross validator
def create_cross_val(pipelineStr, regressor):
    pipelineModel = Pipeline.load(pipelineStr)
    paramGrid = ParamGridBuilder().addGrid(regressor.numTrees, [100, 500]).build()

    crossval = CrossValidator(estimator=pipelineModel,
                            estimatorParamMaps = paramGrid,
                            evaluator= RegressionEvaluator(labelCol = "MSRP"),
                            numFolds=3)
    
    return crossval





if __name__=="__main__":
    spark=SparkSession.builder.appName("BDA Project").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data=spark.read.option("header",True).option("inferSchema",True).csv("Dataset/data.csv")

    

    # Step 1: Describe data
    data.printSchema()
    data.describe().show()
    

    # Step 2: Clean data
    data=data_cleaning(data)
    # cleanedDF.show()

    # Step 3: Creating randomforest pipeline
    pipelineStr,regressor=create_random_pipeline()
    

    # Step 4: Create a crossvalidator
    crossval=create_cross_val(pipelineStr,regressor)
    

    # Step 5: Train and predict
    train_data, test_data = data.randomSplit([0.8,0.2], seed=123)

    cvModel= crossval.fit(train_data)

    bestModel= cvModel.bestModel
    for x in range(len(bestModel.stages)):
        print(bestModel.stages[x])

    pred = cvModel.transform(test_data)
    pred.select("MSRP", "prediction").show()



    # Step 6: Evaluate
    eval = RegressionEvaluator(labelCol = "MSRP")
    rmse = eval.evaluate(pred)
    mse= eval.evaluate(pred, {eval.metricName: "mse"})
    mae= eval.evaluate(pred, {eval.metricName: "mae"})
    r2 = eval.evaluate(pred, {eval.metricName: "r2"})

    print(f"MSE: {mse}")
    print(f"MAE: {mae}")
    print(f"R2: {r2}")

    spark.stop()