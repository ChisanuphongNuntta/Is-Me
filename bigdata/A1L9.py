from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("Cos").getOrCreate()

read_file = spark.read.format("csv").option("header",True).load("fb_live_thailand.csv")

chisanuphongIndex = StringIndexer(inputCols=["status_id","status_type","status_published"],outputCols=["status_idIndex","status_typeIndex","status_publishedIndex"]).fit(read_file).transform(read_file)
chisanuphongAssembler = VectorAssembler(inputCols=["status_idIndex","status_typeIndex","status_publishedIndex"],outputCol="features")

df = chisanuphongAssembler.transform(chisanuphongIndex)
df.show()

output = df.select(df.status_idIndex, df.status_typeIndex, df.status_publishedIndex,df.features)

output.show()

heartTrain,heartTest = output.randomSplit([0.7,0.3])

chisanuphongLinear = LinearRegression(labelCol="status_idIndex",featuresCol="features",predictionCol="new").setMaxIter(10).setRegParam(0.4).setElasticNetParam(0.4)
chisanuphongModel = chisanuphongLinear.fit(heartTrain)

chisanuphongEvalution = chisanuphongModel.evaluate(heartTrain)
print(chisanuphongEvalution.meanSquaredError)
print(chisanuphongEvalution.r2)

datatest = heartTest.select('features')

H = chisanuphongModel.transform(datatest)
H.show()