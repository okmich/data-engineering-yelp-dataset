REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/json-simple-1.1.1.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/elephant-bird-pig-4.14.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/elephant-bird-hadoop-compat-4.14.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/avro-1.8.0.jar' 
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/piggybank-0.15.0.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/jackson-core-asl-1.9.13.redhat-3.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/jackson-mapper-asl-1.9.13.redhat-3.jar'
REGISTER /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-pig-adapter.jar
REGISTER /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar

rawData = LOAD '/user/cloudera/hackerday/yelp/businesses' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as (json:map[]);

business = FOREACH rawData GENERATE (chararray)json#'business_id' AS business_id, (chararray)json#'full_address' AS full_address, (boolean)json#'open' AS open, json#'categories' AS categories,(chararray)json#'city' AS city,(int)json#'review_count' AS review_count,(chararray)json#'name' AS name, json#'neighborhoods' AS neighborhoods,(float)json#'longitude' AS longitude,(chararray)json#'state' AS state,(float)json#'stars' AS stars,(float)json#'latitude' AS latitude;

STORE business INTO 'yelp.business' USING org.apache.hive.hcatalog.pig.HCatStorer();




photoData = LOAD '/user/cloudera/hackerday/yelp/photos' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as (arr:map[]);
photoRltns = FOREACH photoData GENERATE FLATTEN(arr);
