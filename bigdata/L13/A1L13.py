from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, \
    IndexToString
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Heart_L13").getOrCreate()

df = spark.read.format("csv").option("header",True).load("../fb_live_thailand.csv")

df = df.na.drop()

str_ind = StringIndexer(inputCols= ["num_reactions", "num_loves"], \
                        outputCols= ["num_reactions_ind", "num_loves_ind"]).\
                        fit(df)

heart_trans = str_ind.transform(df)

heart_vec_assembler = VectorAssembler(inputCols= ["num_reactions_ind", "num_loves_ind"], \
                                    outputCol= "features")

lr = LinearRegression(labelCol= "num_loves_ind",\
                      featuresCol= "features",
                      predictionCol= "prediction_col").\
    setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)

pipeline = Pipeline(stages=[heart_vec_assembler, lr])

train_data, test_data = heart_trans.randomSplit([0.7,0.3])

lrModel = pipeline.fit(train_data)

predictions = lrModel.transform(test_data)
predictions.show(5)

evaluator = RegressionEvaluator(predictionCol="prediction_col", \
            labelCol="num_loves_ind")

mse = evaluator.setMetricName("mse").evaluate(predictions)
print(f"The MSE for the linear regression model is {mse:0.2f}")

r2 = evaluator.setMetricName("r2").evaluate(predictions)
print(f"The R2 for the linear regression model is {r2:0.2f}")

towrite = predictions.select("status_id",predictions.num_reactions.\
                            cast(IntegerType()), \
                            predictions.prediction_col. \
                            cast(IntegerType()))

towrite.show(5)

wr = IndexToString(inputCol="prediction_col",\
                   outputCol= "pred",\
                    labels= str_ind.labelsArray[1]).\
                        transform(towrite)

wr = wr.select("status_id", "num_reactions", "pred")
wr.write.mode("overwrite").option("header",True).csv("liner_regressions_result")