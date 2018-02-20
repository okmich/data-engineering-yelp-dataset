// FOCUS
// ===================
// Business
// 	category
// 	hours
// Review
// 	Business


// # test spark 2 reading the database
// =============================
// spark-shell --packages mysql:mysql-connector-java:5.1.44


val properties = new java.util.Properties()
properties.put("user", "yelp")
properties.put("password", "password")
val hoursDF = sqlContext.read.jdbc("jdbc:mysql://192.168.8.105/yelp_db","hours", properties)
val categoryDF = sqlContext.read.jdbc("jdbc:mysql://192.168.8.105/yelp_db","category", properties)


//
// spark-shell --packages org.elasticsearch:elasticsearch-spark-13_2.10:6.1.1 --conf spark.es.index.auto.create=true --conf spark.es.nodes=192.168.8.105:9200
//

// Read with spark
// ===================
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

import org.elasticsearch.spark._ 
import org.elasticsearch.spark.sql._ 

val options = Map(
	"es.nodes" -> "192.168.8.105", 
	"es.port" -> "9200", 
	"es.index.auto.create" -> "true", 
	"es.mapping.id" -> "id", 
	"es.batch.size.entries" -> "50000")


///////////////////////////////////////////////////////////////
/////////// LOAD AND INDEX THE USER DATA
///////////////////////////////////////////////////////////////
val longToDateString = udf((dt: Long) => {
	new java.sql.Date(dt).toString
})

val userDF = sqlContext.read.parquet("/user/cloudera/yelp/sqoop/user").
	withColumn("create_date", longToDateString($"yelping_since")).
	drop("yelping_since").cache

userDF.saveToEs("yelp-users/user", options) 



///////////////////////////////////////////////////////////////
// LOAD, DENORMALIZE AND INDEX THE BUSINESS, HOUR AND CATEGORY DATA
///////////////////////////////////////////////////////////////
val getBizDay = udf((h: String) => {
	h.substring(0, h.indexOf("|"))
})

val getOpenHour = udf((h: String) => {
	val times = h.substring(h.indexOf("|") + 1)
	val t = times.split("-")
	if (t.length > 0) t(0) else null
})

val getClosingHour = udf((h: String) => {
	val times = h.substring(h.indexOf("|") + 1)
	val t = times.split("-")
	if (t.length > 1) t(1) else null
})


val hoursDF = sqlContext.read.parquet("/user/cloudera/yelp/sqoop/hours").
	drop("id").
	withColumnRenamed("business_id", "business_id_hour").
	withColumn("business_hours", struct(getBizDay($"hours").as("day"), getOpenHour($"hours").cast(StringType).as("open"), getClosingHour($"hours").cast(StringType).as("close"))).
	drop("hours").
	groupBy("business_id_hour").
	agg(collect_set("business_hours").as("business_hours"))


val categoryDF = sqlContext.read.parquet("/user/cloudera/yelp/sqoop/category").
	drop("id").
	groupBy("business_id").
	agg(collect_set("category").as("categories")).
	withColumnRenamed("business_id", "business_id_cat")


val businessDF = sqlContext.read.parquet("/user/cloudera/yelp/sqoop/business")

val bizDF = businessDF.
	join(categoryDF, $"id" === $"business_id_cat", "left_outer").
	join(hoursDF, $"id" === $"business_id_hour", "left_outer").
	withColumn("geo_location", concat($"latitude", lit(", "), $"longitude")).
	drop("business_id_cat").
	drop("business_id_hour").
	drop("latitude").
	drop("longitude")


bizDF.saveToEs("yelp-biz/business", options) 

///////////////////////////////////////////////////////////////
// LOAD, DENORMALIZE AND INDEX THE BUSINESS AND REVIEW DATA
///////////////////////////////////////////////////////////////
val reviewDF = sqlContext.read.parquet("/user/cloudera/yelp/sqoop/review").
	withColumn("review_date", longToDateString($"date")).
	drop("date")

val businessReviewDF = reviewDF.
	join(bizDF, reviewDF("business_id") === bizDF("id"), "right_outer").
	select(reviewDF("id"), $"user_id", reviewDF("stars"), $"review_date", $"text", $"useful", $"funny", $"cool", $"business_id", $"name", $"neighborhood", $"address", $"city", $"state", $"postal_code", $"geo_location", $"categories", bizDF("stars").as("biz_stars"))


businessReviewDF.saveToEs("yelp-reviews/review", options) 




// Visualizations
// ================================
// 1) Business map using geo_point 
// 2) Top 10 Business Categories
// 4) Business distribution by state
// 5) Average rating of business over time
// 6) Top rated businesses 
// 7) User sign up 