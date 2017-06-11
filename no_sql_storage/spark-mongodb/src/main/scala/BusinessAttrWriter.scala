
import com.mongodb.spark._
import com.mongodb.spark.config._

import org.bson.Document

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._

import scala.collection.JavaConverters._

//run command spark-submit --class BusinessAttrWriter --conf "spark.mongodb.output.uri=mongodb://192.168.189.1/hackerday.attributes" target/scala-2.10/yelp-spark-mongodb-assembly-1.0.jar  /user/cloudera/rawdata/yelp/businesses conform
// read https://docs.mongodb.com/spark-connector/v1.1/getting-started/  for how to configure the
// collection and uri in the code using WriteConfig
object BusinessAttrWriter extends java.io.Serializable {

	def main(args: Array[String]) = {
		val sparkConf = new SparkConf
		val sc = new SparkContext(sparkConf)

		val businessRDD = sc.textFile(args(0))
		val conformFlag = if (args(1) == "conform") true else false

		val businessAttributesRDD = businessRDD mapPartitions (convertToAttrs(_, conformFlag))

		MongoSpark.save(businessAttributesRDD)
	}


	def convertToAttrs(it: Iterator[String], cf: Boolean) : Iterator[Document] ={
		/**
		 * conform the key to a standard
		 *
		 */
		def conformKeys(attJson : JSONObject) : Map[String, Object] = {
			val attJsonMap = attJson.asInstanceOf[java.util.HashMap[String,Object]].asScala
			val conformed=attJsonMap map (t => {
				val value = t._2 match {
					case _ : JSONObject => conformKeys(t._2.asInstanceOf[JSONObject])
					case _ => t._2
				}
				(t._1.replace(" ", "_").toLowerCase, value)
			})
			conformed.toMap
		}
		/**
		 * populate the BSON Document with the entries of this map object
		 *
		 */
		def populateDoc(jsonMap : Map[String, Any], doc: Document) : Unit = {
			jsonMap foreach (t => {
				val value = t._2 match {
					case _ : Map[String, Any] => {
						var childDoc = new Document()
						populateDoc(t._2.asInstanceOf[Map[String, Any]], childDoc)
						childDoc
					}
					case _ => t._2
				}
				doc.put(t._1, value)
			})
		}


		val parser = new JSONParser();

		it map (item => {
			val obj = (parser.parse(item)).asInstanceOf[JSONObject]
			val attJson = obj.get("attributes").asInstanceOf[JSONObject]
			val businessId = obj.get("business_id").asInstanceOf[String]

			val doc = if (cf) {
				val _doc = new Document("_id", businessId)
				populateDoc(conformKeys(attJson), _doc) 
				_doc
			} else {
				val _doc = Document.parse(attJson.toJSONString)
				_doc.put("_id", businessId)
				_doc
			}

			doc
			})
	}
}