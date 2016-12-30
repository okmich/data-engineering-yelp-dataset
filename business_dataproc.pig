REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/json-simple-1.1.1.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/elephant-bird-pig-4.14.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/elephant-bird-hadoop-compat-4.14.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/avro-1.8.0.jar' 
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/piggybank-0.15.0.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/jackson-core-asl-1.9.13.redhat-3.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/jackson-mapper-asl-1.9.13.redhat-3.jar'
REGISTER '/home/cloudera/Classes/data-engineering-yelp-dataset/jars/custome-pig-udf-0.0.1-SNAPSHOT.jar'


raw_tweets = LOAD '/user/cloudera/hackerday/yelp/businesses' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as (json:map[]);





STORE denormalized INTO 'yelp.business' USING org.apache.HCatalog.pig.HCatStorer();