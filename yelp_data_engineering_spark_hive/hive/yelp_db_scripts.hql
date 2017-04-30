create database yelp;

create external table tip (
	user_id string,
	business_id string,
	likes bigint,
	text string,
	date string
)
stored as parquet
location '/user/cloudera/output/yelp/tips';


