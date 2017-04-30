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


create external table review (
    review_id string,
    user_id string,
    business_id string,
    stars bigint,
    text string,
    date string,
    cool bigint,
    funny bigint,
    useful bigint
)
stored as parquet
location  '/user/cloudera/output/yelp/reviews';