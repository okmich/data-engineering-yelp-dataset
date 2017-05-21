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
location  '/user/cloudera/output/yelp/categories';


create external table business_category (
    business_id string,
    category_id int
)
stored as parquet
location  '/user/cloudera/output/yelp/business_categories';

-- to get a distribution of business by category
select c.description, count(1) from business_category bc join categories c on c.id = bc.category_id group by c.description;
-- to get a distribution of business by city
select city, count(1) from business group by city having count(1) > 30;


create external table business_hour (
    business_id string,
    sunday struct<close:string,open:string>,
    monday struct<close:string,open:string>,
    tuesday struct<close:string,open:string>,
    wednesday struct<close:string,open:string>,
    thursday struct<close:string,open:string>,
    friday struct<close:string,open:string>,
    saturday struct<close:string,open:string>
)
stored as parquet
location  '/user/cloudera/output/yelp/business_hours';


create view v_business_hour as 
select business_id, sunday.open as sunday_open, sunday.close as sunday_close
, monday.open as monday_open, monday.close as monday_close
, tuesday.open as tuesday_open, tuesday.close as tuesday_close
, wednesday.open as wednesday_open, wednesday.close as wednesday_close
, thursday.open as thursday_open, thursday.close as thursday_close
, friday.open as friday_open, friday.close as friday_close
, saturday.open as saturday_open, saturday.close as saturday_close
from business_hour;

create view v_review as 
    select review_id,
        user_id,
        business_id,
        stars,
        text,
        `date`,
        cool,
        funny,
        useful,
        cast(substring(`date`, 1, 4) as int) year,
        cast(substring(`date`, 6, 2) as int) month, //03, March
        cast(substring(`date`, 9, 2) as int) day //01 Sunday
from review;

drop table review;
--remember to delete the /user/cloudera/output/yelp/reviews hdfs directory

create external table review (
    review_id string,
    user_id string,
    business_id string,
    stars bigint,
    text string,
    date string,
    cool bigint,
    funny bigint,
    useful bigint,
    year int,
    month int,
    day int,
    month_name string,
    dayofweek string,
    weekofyear int
)
stored as parquet
location  '/user/cloudera/output/yelp/reviews';
