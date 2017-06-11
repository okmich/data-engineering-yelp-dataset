package importer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.functions._
import udf.functions._

object TipImporter extends Importer {

	def doImport(hiveCtx: HiveContext, inputDir: String) : Unit = {
		import hiveCtx.implicits._

		val tipDF = hiveCtx.jsonFile(inputDir)
		val refinedTipDF = tipDF.select($"user_id",$"business_id",$"likes",$"text",$"date", substring($"date", 1, 4).cast("int").as("year"), substring($"date", 6, 2).cast("int").as("month"),  substring($"date", 9, 2).cast("int").as("day"), monthName(substring($"date", 6, 2)).as("monthname"), dayOfWeek($"date").as("dayofweek"), weekofyear(substring($"date", 1, 4)).as("weekofyear")).coalesce(1)

		refinedTipDF.write.insertInto("tip")
	}
}