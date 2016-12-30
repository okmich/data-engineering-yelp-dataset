create database yelp;

use yelp;
add jar /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;

CREATE EXTERNAL TABLE tip (
	user_id string,
	text string,
	likes tinyint,
	date string,
	type string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/cloudera/hackerday/yelp/tips';

CREATE EXTERNAL TABLE review (
	votes map<string, string>,
	user_id string,
	review_id string,
	stars tinyint,
	date string,
	text string,
	type string,
	business_id string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/cloudera/hackerday/yelp/reviews';


CREATE EXTERNAL TABLE checkin (
	checkin_info map<string, string>,
	type string,
	business_id string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/cloudera/hackerday/yelp/checkins';


CREATE EXTERNAL TABLE user (
	yelping_since string,
	votes map<string, string>,
	review_count int,
	name string,
	user_id string,
	friends array<string>,
	fans int,
	average_stars float,
	type string,
	compliments map<string, string>,
	elite array<string>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/cloudera/hackerday/yelp/users';