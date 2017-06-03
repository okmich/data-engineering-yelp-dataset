create database yelp;

create external table user_community (
	user_id string,
	community_id bigint)
stored as parquet
location '/user/cloudera/output/yelp/user_community';