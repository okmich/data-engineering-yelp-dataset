spark.neo4j.bolt.url=bolt://192.168.189.1
spark.neo4j.bolt.user=neo4j
spark.neo4j.bolt.password=password


spark-shell --master yarn --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1 --conf spark.neo4j.bolt.url=bolt://192.168.189.1 --conf spark.neo4j.bolt.user=neo4j --conf spark.neo4j.bolt.password=password

sc.getConf.get("spark.neo4j.bolt.url")
sc.getConf.get("spark.neo4j.bolt.user")
sc.getConf.get("spark.neo4j.bolt.password")

import org.neo4j.spark._

val g = Neo4jGraph.loadGraph(sc, "User", Seq("FRIEND"), "User")


val sparkConf = new SparkConf()
sparkConf.set()

val result = Neo4jGraph.saveGraph(sc, graph, "user_id", "FRIEND")