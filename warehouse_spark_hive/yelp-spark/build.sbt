name := "yelp-spark-processor"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.0"
//libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1"
