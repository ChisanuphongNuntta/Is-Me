# from pyspark.ml import SparkSession
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql.functions import desc, col
spark = SparkSession.builder.appName("Cos").getOrCreate()

vertices = spark.createDataFrame([
    ("Alice",45),
    ("Jacob",43),
    ("Roy",21),
    ("Ryan",49),
    ("Emily",24),
    ("Sheldon",52)],
    ["id","age"]
)

edges = spark.createDataFrame([("Sheldon","Alice","Sister"),
                               ("Alice","Jacob","Husband"),
                                ("Emily", "Jacob","Father"),
                                ("Ryan","Alice","Friend"),
                                ("Alice", "Emily", "Daughter"),
                                ("Alice","Roy","Son"),
                                ("Jacob","Roy","Son")],
                                ["src", "dst", "relation"])

heart_graph = GraphFrame(vertices, edges)
heart_graph.edges.groupBy('src','dst').count().oderBy(desc('cont')).show(5)
heart_graph.edges.where("src = 'heart'").groupBy('src','dst').count().orderBy(desc('count')).show(5)

heart_query = heart_graph.edges.where("src = 'heart'")
heart_supgraph = GraphFrame(heart_graph.vertices, heart_query)

motifs = heart_graph.find("(a) - [ab] -> (b)")
motifs = heart_graph.find('(a) - [ab] -> (b)').filter("ab.relation = 'some value'")

heart_rank = heart_graph.pageRank(resetProbability=0.15,maxIter=5)
heart_rank.vertices.orderBy(desc('pagerank')).show(5)

heart_in_degree = heart_graph.inDegrees
heart_in_degree.orderBy(desc('inDegree')).show(5)

spark.sparkContext.setCheckpointDir('/tmp/checkpoints')

heart_cc = heart_graph.connectedComponents()
heart_scc = heart_graph.stronglyConnectedComponents(maxIter=5)

heart_graph.bfs(fromExpr="id = '5'",toExpr = "id = '4'",maxPathLength = 2).show()