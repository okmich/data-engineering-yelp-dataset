// read the tip json data
// transform it
// store it to hdfs 
val tipDF = sqlContext.jsonFile("/user/cloudera/rawdata/yelp/tips")
//get number of partition
tipDF.partitions
val refinedTipDF = tipDF.select("user_id","business_id","likes","text","date").repartition(1)
refinedTipDF.write.parquet("/user/cloudera/output/yelp/tips")
//we build the hive tip table on top of this output to allow users query our data


import org.apache.spark.sql.Row


//integrating spark and hive using the hive context
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new HiveContext(sc)
import hiveCtx.implicits._

hiveCtx.sql("use yelp")
val tipDF = hiveCtx.read.table("tip")
//create a managed table in hive's yelp database
hiveCtx.sql("use yelp")
tipDF.write.saveAsTable("tip2_from_spark")



//write directly to a hive table already created in hive.
//care must be taken to ensure that the structure and column data type from the dataframe matches the hive table
val reviewDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/reviews").
		select("review_id","user_id","business_id","stars","text","date","votes.cool","votes.funny","votes.useful")
reviewDF.write.insertInto("yelp.review")


// normalizing the user dataset to a many-to-many self referencing model
val userDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/users")

val friends = userDF.select("user_id", "friends").rdd


def parseRow(row: Row) : Seq[(String, String)] ={
	val user_Id = row.getAs[String](0)
	val fList = row.getAs[scala.collection.mutable.WrappedArray[String]](1)

	fList.map(s => (user_Id, s))
}

val userFriendRDD = friends.flatMap(parseRow(_))

val userfriendDF = userFriendRDD.toDF
//write into database table
userfriendDF.write.insertInto("user_friends")


val userMainDF = userDF.select("user_id","name","review_count","yelping_since","votes.useful","votes.funny","votes.cool","fans","elite","average_stars","compliments.hot",  "compliments.more", "compliments.profile", "compliments.cute", "compliments.list", "compliments.note", "compliments.plain", "compliments.cool", "compliments.funny", "compliments.writer", "compliments.photos")
//importance of coalesce
userMainDF.coalesce(1).write.insertInto("user")
 
//  select u.user_id, u.fans, g.num from user u join 
// (select user_id, count(1) num from user_friends group by user_id) g
// on g.user_id = u.user_id

//a simulation of incremental dataset
val trialDF = databaseUserDF.sample(false, 0.1)

val allDF = databaseUserDF.unionAll(trialDF)

val newDataset = allDF.
	map((row: Row) => (row.getAs[String](1), row)).
	reduceByKey(function)


//businesses
val bizDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/businesses").cache

//to get the complex schema
val schema = bizDF.schema.prettyJson


// import java.nio.file._

// Files.write(Paths.get("file:///home/cloudera/classes/schema.json"), schema.getBytes, StandardOpenOption.CREATE_NEW)
// val path = Paths.get(new java.net.URI("file:///home/cloudera/classes/businesses_schema.json"))
// Files.write(path, schema.getBytes, StandardOpenOption.CREATE_NEW)

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

catRdd.toDF.write.insertInto("categories")

val catDF = hiveCtx.read.table("categories")



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
//make decisions on business_attributes

import org.apache.spark.sql.functions._


val reviewDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/reviews")
val revReviewDF = reviewDF.select($"review_id",$"user_id",$"business_id",$"stars",$"text",$"date",$"votes.cool",$"votes.funny",$"votes.useful", substring($"date", 1, 4).cast("int").as("year"), substring($"date", 6, 2).cast("int").as("month"),  substring($"date", 9, 2).cast("int").as("day"), monthName(substring(reviewDF("date"), 6, 2)).as("monthname"), dayOfWeek(reviewDF("date")).as("dayofweek"), weekofyear(substring(reviewDF("date"), 1, 4)).as("weekofyear"))

revReviewDF.coalesce(5).write.insertInto("yelp.review")



val tipDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/tips")
val refinedTipDF = tipDF.select($"user_id",$"business_id",$"likes",$"text",$"date", substring($"date", 1, 4).cast("int").as("year"), substring($"date", 6, 2).cast("int").as("month"),  substring($"date", 9, 2).cast("int").as("day"), monthName(substring($"date", 6, 2)).as("monthname"), dayOfWeek($"date").as("dayofweek"), weekofyear(substring($"date", 1, 4)).as("weekofyear")).coalesce(1)

refinedTipDF.write.insertInto("tip")