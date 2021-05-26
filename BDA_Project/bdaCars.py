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
    data = data.drop("Market Category")
    data = data.na.drop()
    
    print("Number of rows After cleaning")
    print((data.count(), len(data.columns)))
    
    return data



# create randomforest pipeline
def create_random_pipeline():
    print("Creating Data pipeline for regressor")
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
    print("Creating cross validator")
    pipelineModel = Pipeline.load(pipelineStr)
    paramGrid = ParamGridBuilder().addGrid(regressor.numTrees, [100, 500]).build()

    crossval = CrossValidator(estimator=pipelineModel,
                            estimatorParamMaps = paramGrid,
                            evaluator= RegressionEvaluator(labelCol = "MSRP"),
                            numFolds=3)
    
    return crossval


# Train the model
def train_model(data, crossval):
    print("Training model on car dataset")
    train_data, test_data = data.randomSplit([0.8,0.2], seed=123)

    cvModel= crossval.fit(train_data)

    bestModel= cvModel.bestModel
    for x in range(len(bestModel.stages)):
        print(bestModel.stages[x])

    pred = cvModel.transform(test_data)
    pred.select("MSRP", "prediction").show()

    return pred



# Main 
if __name__=="__main__":

    # Create spark session
    spark=SparkSession.builder.appName("BDA Project").master("local[*]").\
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    
    # Read data and print schema
    data=spark.read.option("header",True).option("inferSchema",True).csv("Dataset/data.csv")
    data.printSchema()      #Schema of data
    
    
    # Step 3: Describe data
    print("Data Description")
    data.select(
        "Year",
        "Engine HP",
        "Popularity",
        "Model",
        "MSRP"
    ).describe().show(5)


    # Step 4: Clean data
    data=data_cleaning(data)
    

    # Step 3: Creating randomforest pipeline
    pipelineStr,regressor=create_random_pipeline()
    

    # Step 4: Create a crossvalidator
    crossval=create_cross_val(pipelineStr,regressor)
    

    # Step 5: Train and predict
    pred=train_model(data,crossval)
    



    # Step 6: Evaluate
    eval = RegressionEvaluator(labelCol = "MSRP")
    rmse = eval.evaluate(pred)
    mse= eval.evaluate(pred, {eval.metricName: "mse"})
    mae= eval.evaluate(pred, {eval.metricName: "mae"})
    r2 = eval.evaluate(pred, {eval.metricName: "r2"})

    print(f"R2 Score: {r2}")
    print(f"MSE: {mse}")
    print(f"MAE: {mae}")
    

    spark.stop()