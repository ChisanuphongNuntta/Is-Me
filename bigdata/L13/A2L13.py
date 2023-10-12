from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql.functions import desc, col , lit

spark = SparkSession.builder.appName("GrapgAnalysis").getOrCreate()

df = spark.read.format("csv").option("header",True).load("airline_routes.csv")
df.show(5)

vertices = df.withColumnRenamed("source_airport", "id")

edges = df.withColumnRenamed("source_airport", "src").\
        withColumnRenamed("distination_airport", "dst")

vertices.show(5)
edges.show(5)

graph = GraphFrame(vertices, edges)
print("Number of vertices",str(graph.vertices.count()))
print("Number of edges", str(graph.edges.count()))

get_graph = graph.edges.groupBy("src","dst").count().\
    where("count > 5").\
    oderBy(desc("count")).\
    withColum("source_color", lit("#3358FF")).\
    withColum("destination_color", lit("#FF3F33"))

get_graph.show()
print("Number of edgs to be written", str(get_graph.count()))

get_graph.write.mode("overwrite").option("header",True).\
    csv("get_graph")