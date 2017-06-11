import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel._

import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new HiveContext(sc)
hiveCtx.sql("use yelp")

// normalizing the user dataset to a many-to-many self referencing model
val userDF = sqlContext.jsonFile("/user/cloudera/rawdata/yelp/users").cache

val friends = userDF.select("user_id", "friends").rdd

def parseRow(row: Row) : Seq[(String, String)] ={
	val user_Id = row.getAs[String](0)
	val fList = row.getAs[scala.collection.mutable.WrappedArray[String]](1)

	fList.map(s => (user_Id, s))
}
// RDD[(String,String)]
val userFriendRDD = friends.flatMap(parseRow(_)).cache

//get a user id
val userIdRDD = userDF.select("user_id").rdd map((r : Row) => r.getAs[String](0))
val userWithIndexRDD = userIdRDD.zipWithIndex
//a non-spark managed map to lookup VertexId from user_id
val userIDMap = userWithIndexRDD.collect.toMap
//a non-spark managed map to lookup user_id from VertexId
val idUserMap = userIDMap map (t => (t._2, t._1))

import org.apache.spark.graphx._
//do a flip of the userWithIndexRDD
val vertexTupleRDD : RDD[(VertexId, String)] = userWithIndexRDD.map(t => (t._2, t._1))
val vertexRDD = VertexRDD(vertexTupleRDD)

val friendships = userFriendRDD.map(uf => new Edge(userIDMap(uf._2), userIDMap(uf._1), ""))
val edgeRDD = EdgeRDD.fromEdges(friendships)


val graph = Graph(vertexRDD, edgeRDD,defaultVertexAttr="",edgeStorageLevel=DISK_ONLY_2,vertexStorageLevel=DISK_ONLY_2)

//number of relationships/friendships
graph.numEdges

//number of users/node/vertices
graph.numVertices

val outDeg = graph.outDegrees
val inDeg = graph.inDegrees
val inDegDesc = inDeg.sortBy(v=> -1 * v._2).map(it => idUserMap(it._1) + " has indeg of " + it._2.toString)
inDegDesc.take(20) foreach println

import hiveCtx.implicits._
val cc = graph.connectedComponents
val communityRDD = cc.vertices map((t : (VertexId, VertexId)) => (idUserMap(t._1), t._2.toLong))
val communityDF = communityRDD.toDF
//write to a hive table
communityDF.write.mode("overwrite").insertInto("user_community")

val cc = graph.stronglyConnectedComponents