package importer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.functions._
import udf.functions._

object BusinessImporter extends Importer {

	def doImport(hiveCtx: HiveContext, inputDir: String) : Unit = {
		def parseRow(row: Row) : Seq[(String, String)] ={
			val user_Id = row.getAs[String](0)
			val fList = row.getAs[scala.collection.mutable.WrappedArray[String]](1)

			fList.map(s => (user_Id, s))
		}

		import hiveCtx.implicits._
		
		val bizDF = hiveCtx.jsonFile(inputDir).cache

		val businessDF = bizDF.select("business_id","categories","city","full_address","latitude","longitude","name","neighborhoods","open","review_count","stars","state")

		val catRdd = bizDF.select("categories").rdd.flatMap(row => row.getAs[scala.collection.mutable.WrappedArray[String]](0).toSeq).
		    distinct.
		    sortBy(i => i).
		    coalesce(1).  //change the rdd to a partition of n
		    mapPartitions(itr => {
		        var index = 0
		        itr map (i => {
		            index = index + 1
		            (index, i)
		        })
		    })

		val catDF = catRdd.toDF
		//write to hive table
		catDF.coalesce(1).write.insertInto("categories")

		//creating the many-to-many business_category
		//create a map of category to id (NightLife -> 10)
		val catMap = catDF.rdd.map((row: Row) => (row.getAs[String](1)->row.getAs[Integer](0))).collect.toMap
		//(XXXXXXXXXX, NightLife)
		val bizCatRDD = bizDF.select("business_id","categories").flatMap(parseRow(_))
		//(XXXXXXXXXX, 10)
		val bizCat = bizCatRDD.map(t => (t._1, catMap(t._2))).toDF

		//insert into business_category table
		bizCat.toDF.coalesce(1).write.insertInto("business_category")

		//create businesses
		businessDF.coalesce(1).write.insertInto("business")

		//creating the many-to-many business_hours
		val bizHoursDF =  bizDF.select("business_id", "hours.Sunday","hours.Monday","hours.Tuesday","hours.Wednesday","hours.Thursday","hours.Friday","hours.Saturday")

		bizHoursDF.coalesce(1).write.insertInto("business_hour")
	}
}