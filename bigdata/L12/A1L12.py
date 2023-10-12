from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import seaborn as sns
from pyspark.sql.types import IntegerType, DoubleType
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, desc

spark = SparkSession \
    .builder \
    .appName("testLinearRegression") \
    .getOrCreate()

df = spark.read.format("csv"). \
    option("header",True). \
    load("../fb_live_thailand.csv")

str_ind = StringIndexer(inputCols= ["num_reactions", "num_loves"], \
                        outputCols= ["num_reactions_ind", "num_loves_ind"]).\
                        fit(df).transform(df)


heart_vec_assembler = VectorAssembler(inputCols= ["num_reactions_ind", "num_loves_ind"], \
                                    outputCol= "features")    

lr = LinearRegression(labelCol= "num_loves_ind",\
                      featuresCol= "features",
                      predictionCol= "prediction_col").\
    setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.8)

pipeline = Pipeline(stages=[heart_vec_assembler, lr])

train_data, test_data = str_ind.randomSplit([0.7,0.3])

lrModel = pipeline.fit(train_data)

predictions = lrModel.transform(test_data)
predictions.show(5)

evaluator = RegressionEvaluator(predictionCol="prediction_col", \
            labelCol="num_loves_ind")

mse = evaluator.setMetricName("mse").evaluate(predictions)
print(f"The MSE for the linear regression model is {mse:0.2f}")

r2 = evaluator.setMetricName("r2").evaluate(predictions)
print(f"The R@ for the linear regression model is {mse:0.2f}")

df = df.select(df.num_reactions.cast(DoubleType()),\
               df.num_loves.cast(DoubleType()))

sns.lmplot(x = "num_reactions", y = "num_loves", \
           data = df.toPandas())

data_toplot = predictions.select(\
    predictions.num_loves.cast(IntegerType()),\
        predictions.prediction_col.cast(IntegerType())).\
            orderBy(col("prediction_col").desc())
data_toplot.show()

sns.lmplot(x = "num_loves", y = "prediction_col", \
           data = data_toplot.toPandas())

plt.show()


