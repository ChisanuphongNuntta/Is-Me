from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder.appName("Cos").getOrCreate()
read_file = spark.read.format("csv").option("header",True).option('inferSchema',True).load("book_ratings.csv")
read_file.show()

dataTrain,dataTest = read_file.randomSplit([0.7,0.3])

chisanuphong_ALS = ALS(maxIter= 10, userCol= "user_id",itemCol="book_id",ratingCol="rating",coldStartStrategy="drop")
chisanuphong_model = chisanuphong_ALS.fit(dataTrain)
chisanuphong_prediction = chisanuphong_model.transform(dataTest)

chisanuphong_prediction.show()

chisanuphong_evaluator = RegressionEvaluator(metricName='rmse',labelCol='rating',predictionCol="prediction")
chisanuphong_rmse = chisanuphong_evaluator.evaluate(chisanuphong_prediction)
print(chisanuphong_rmse)

chisanuphong_user = dataTest.filter(dataTest["user_id"] == 540)
chisanuphong_user.show()

chisanuphong_recommendation = chisanuphong_model.transform(chisanuphong_user)
chisanuphong_recommendation.show()

chisanuphong_model.recommendForAllItems(5).show(truncate=False)
chisanuphong_model.recommendForAllUsers(5).show(truncate=False)