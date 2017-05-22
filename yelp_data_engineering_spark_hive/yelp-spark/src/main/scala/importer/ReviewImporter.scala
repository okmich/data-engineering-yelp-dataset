package importer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.functions._
import udf.functions._

object ReviewImporter extends Importer {

	def doImport(hiveCtx: HiveContext, inputDir: String) : Unit = {
		import hiveCtx.implicits._

		val reviewDF = hiveCtx.jsonFile(inputDir)
		val revReviewDF = reviewDF.select($"review_id",$"user_id",$"business_id",$"stars",$"text",$"date",$"votes.cool",$"votes.funny",$"votes.useful", substring($"date", 1, 4).cast("int").as("year"), substring($"date", 6, 2).cast("int").as("month"),  substring($"date", 9, 2).cast("int").as("day"), monthName(substring(reviewDF("date"), 6, 2)).as("monthname"), dayOfWeek(reviewDF("date")).as("dayofweek"), weekofyear(substring(reviewDF("date"), 1, 4)).as("weekofyear"))

		revReviewDF.coalesce(5).write.insertInto("review")
			
	}
}