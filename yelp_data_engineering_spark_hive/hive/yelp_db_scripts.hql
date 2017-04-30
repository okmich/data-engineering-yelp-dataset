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

create external table user (
    user_id string,
    name string,
    review_count bigint,
    yelping_since string,
    useful bigint,
    funny bigint,
    cool bigint,
    fans bigint,
    elite array<bigint>,
    average_stars double,
    compliment_hot bigint,
    compliment_more bigint,
    compliment_profile bigint,
    compliment_cute bigint,
    compliment_list bigint,
    compliment_note bigint,
    compliment_plain bigint,
    compliment_cool bigint,
    compliment_funny bigint,
    compliment_writer bigint,
    compliment_photos bigint
)
stored as parquet
location  '/user/cloudera/output/yelp/user';


create external table user_friends (
    user_id string,
    friend_id string
)
stored as parquet
location  '/user/cloudera/output/yelp/user_friends';


create external table business (
    business_id string,
    categories array<string>,
    city string,
    full_address string,
    latitude double,
    longitude double,
    name string,
    neighborhoods array<string>,
    open boolean,
    review_count bigint,
    stars double,
    state string
)
stored as parquet
location  '/user/cloudera/output/yelp/business';




create external table categories (
    id int,
    description string
)
stored as parquet
location  '/user/cloudera/output/yelp/business_categories';