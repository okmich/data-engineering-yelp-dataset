package importer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.functions._
import udf.functions._

object UserImporter extends Importer {

	def doImport(hiveCtx: HiveContext, inputDir: String) : Unit = {
		import hiveCtx.implicits._

		// normalizing the user dataset to a many-to-many self referencing model
		val userDF = hiveCtx.jsonFile(inputDir).cache

		val friends = userDF.select("user_id", "friends").rdd

		def parseRow(row: Row) : Seq[(String, String)] ={
			val user_Id = row.getAs[String](0)
			val fList = row.getAs[scala.collection.mutable.WrappedArray[String]](1)

			fList.map(s => (user_Id, s))
		}

		val userfriendDF = friends.flatMap(parseRow(_)).toDF
		//write into database table
		userfriendDF.write.insertInto("user_friends")

		val userMainDF = userDF.select("user_id","name","review_count","yelping_since","votes.useful","votes.funny","votes.cool","fans","elite","average_stars","compliments.hot",  "compliments.more", "compliments.profile", "compliments.cute", "compliments.list", "compliments.note", "compliments.plain", "compliments.cool", "compliments.funny", "compliments.writer", "compliments.photos")
		//importance of coalesce
		userMainDF.coalesce(1).write.insertInto("user")
	}
}