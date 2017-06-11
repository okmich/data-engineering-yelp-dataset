
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.hive.HiveContext

import importer._

object Main {

	def main(args: Array[String]) : Unit = {
		//business input_path
		val importType = args(0) //tip, user, business, review
		val inputPathDir = args(1)
		val dbName = args(2)

		val sparkConf = new SparkConf()
		val sparkCtx = new SparkContext(sparkConf)

		val hiveCtx = new HiveContext(sparkCtx)
		//default to the database
		hiveCtx.sql(s"use $dbName")

		val specImporter: Importer = importType.toLowerCase match {
			case "business" => BusinessImporter
			case "tip" => TipImporter
			case "review" => ReviewImporter
			case "user" => UserImporter
			case _ => null
		}

		if (specImporter != null)
				specImporter.doImport(hiveCtx, inputPathDir)
	}
}