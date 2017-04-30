// read the tip json data
// transform it
// store it to hdfs 
val tipDF = sqlContext.jsonFile("/user/cloudera/rawdata/yelp/tips")
val refinedTipDF = tipDF.select("user_id","business_id","likes","text","date").repartition(1)
refinedTipDF.write.parquet("/user/cloudera/output/yelp/tips")
//we build the hive tip table on top of this output to allow users query our data


import org.apache.spark.sql.Row


//integrating spark and hive using the hive context
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new HiveContext(sc)
import hiveCtx.implicits._


val tipDF = hiveCtx.read.table("tip")
//create a managed table in hive's yelp database
hiveCtx.sql("use yelp")
tipDF.write.saveAsTable("tip2_spark")



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


val userfriendDF = friends.flatMap(parseRow(_)).toDF
//write into database table
userfriendDF.write.insertInto("user_friends")


val userMainDF = userDF.select("user_id","name","review_count","yelping_since","votes.useful","votes.funny","votes.cool","fans","elite","average_stars","compliments.hot",  "compliments.more", "compliments.profile", "compliments.cute", "compliments.list", "compliments.note", "compliments.plain", "compliments.cool", "compliments.funny", "compliments.writer", "compliments.photos")

 userMainDF.write.insertInto("user")
 
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
val bizDF = hiveCtx.jsonFile("/user/cloudera/rawdata/yelp/businesses")

//to get the complex schema
val schema = bizDF.schema.prettyJson


// import java.nio.file._

// Files.write(Paths.get("file:///home/cloudera/classes/schema.json"), schema.getBytes, StandardOpenOption.CREATE_NEW)
// val path = Paths.get(new java.net.URI("file:///home/cloudera/classes/businesses_schema.json"))
// Files.write(path, schema.getBytes, StandardOpenOption.CREATE_NEW)

bizDF.select("business_id","categories","city","full_address","latitude","longitude","name","neighborhoods","open","review_count","stars","state")

val catRdd = bizDF.select("categories").rdd.flatMap(row => row.getAs[scala.collection.mutable.WrappedArray[String]](0).toSeq).
    distinct.
    sortBy(i => i).
    coalesce(1).
    mapPartitions(itr => {
        var index = 0
        itr map (i => {
            index = index + 1
            (index, i)
        })
    })

catRdd.toDF.write.insertInto("categories")


val bizCatRDD = bizDF.select("business_id","categories").flatMap(parseRow(_))


//creating the many-to-many business_category
//create businesses

//make decisions on business_attributes

