name := "yelp-spark-mongodb"
version := "1.0"
scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1"
libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.10" % "1.1.0"
