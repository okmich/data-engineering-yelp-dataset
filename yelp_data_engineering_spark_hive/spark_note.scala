// read the tip json data
// transform it
// store it to hdfs 
val tipDF = sqlContext.jsonFile("/user/cloudera/rawdata/yelp/tips")
val refinedTipDF = tipDF.select("user_id","business_id","likes","text","date").repartition(1)
refinedTipDF.write.parquet("/user/cloudera/output/yelp/tips")
//we build the hive tip table on top of this output to allow users query our data




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
val userDF = sqlContext.jsonFile("/user/cloudera/rawdata/yelp/users")