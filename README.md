# cis5200-project
Data Science Project

HIVE


Q1 Which companies have the highest earnings in Chicago in 2016? 


DROP TABLE IF EXISTS area;

CREATE EXTERNAL TABLE IF NOT EXISTS area(trip_id int, 
trip_start_timestamp timestamp, 
trip_end_timestamp timestamp, 
trip_seconds Int, 
trip_miles float, 
pickup_census_tract Int, 
dropoff_census_tract Int, 
pickup_community_area Int, 
dropoff_community_area Int, 
fare float, 
tips float, 
tolls float, 
extras float, 
trip_total float, 
payment_type String, 
company Int, 
pickup_latitude Int, 
pickup_longitude Int, 
dropoff_latitude Int, 
dropoff_longitude Int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/project1'
TBLPROPERTIES("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS Company2
as
select company, sum(trip_total) as Total_F, MAX(tips)
from area
Group by company
ORDER BY Total_F DESC LIMIT 50;

Q2 Which taxi-ids generate the highest fare with the respective number of rides?


DROP TABLE IF EXISTS area;

CREATE EXTERNAL TABLE IF NOT EXISTS area(trip_id int, 
trip_start_timestamp timestamp, 
trip_end_timestamp timestamp, 
trip_seconds Int, 
trip_miles float, 
pickup_census_tract Int, 
dropoff_census_tract Int, 
pickup_community_area Int, 
dropoff_community_area Int, 
fare float, 
tips float, 
tolls float, 
extras float, 
trip_total float, 
payment_type String, 
company Int, 
pickup_latitude Int, 
pickup_longitude Int, 
dropoff_latitude Int, 
dropoff_longitude Int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/project1'
TBLPROPERTIES("skip.header.line.count"="1");


create table if not exists taxi
as
select taxi_id, count(taxi_id) as cnt, pickup_community_area, count(pickup_community_area) as total_pickup, dropoff_community_area, count(dropoff_community_area ) as dropoff, SUM(trip_total) as sum
from area
group by taxi_id, pickup_community_area, dropoff_community_area
order by sum desc limit 20;

Q3 What is the total monthly comparison between Credit and Cash payments?

DROP TABLE IF EXISTS payment;

CREATE EXTERNAL TABLE IF NOT EXISTS payment
(trip_id int, trip_start_timestamp timestamp, trip_end_timestamp timestamp, trip_seconds Int, trip_miles float, pickup_census_tract Int, dropoff_census_tract Int, pickup_community_area Int, dropoff_community_area Int, fare float, tips float, tolls float, extras float, trip_total float, payment_type String, company Int, pickup_latitude Int, pickup_longitude Int, dropoff_latitude Int, dropoff_longitude Int)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/user/hbatra/project' 
TBLPROPERTIES("skip.header.line.count"="1");


LOAD DATA INPATH '/user/hbatra/chicago/chicago_taxi_trips_2016_01.csv’ OVERWRITE INTO TABLE payment;

hdfs dfs -ls chicago/chicago_taxi_trips_2016_01.csv

Drop table if exists payment_1;

Create external table payment_1 
(payment_type String,fare float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/user/hbatra/payment_1/';

INSERT OVERWRITE TABLE payment_1 
Select payment_type,sum(fare) as Total_fare from payment group by payment_type,order by Total_fare;

Repeat the same for all rest 11 months

Q4 What is the total number of monthly pick-ups in Chicago(2016)?

CREATE EXTERNAL TABLE IF NOT EXISTS frequency
(trip_id int, trip_start_timestamp timestamp, trip_end_timestamp timestamp, trip_seconds Int, trip_miles float, pickup_census_tract Int, dropoff_census_tract Int, pickup_community_area Int, dropoff_community_area Int, fare float, tips float, tolls float, extras float, trip_total float, payment_type String, company Int, pickup_latitude Int, pickup_longitude Int, dropoff_latitude Int, dropoff_longitude Int)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/user/hbatra/project1' 
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hbatra/chicago/chicago_taxi_trips_2016_01.csv' OVERWRITE INTO TABLE frequency;
hdfs dfs -ls chicago/chicago_taxi_trips_2016_01.csv

Drop table if exists frequency_1;

Create external table frequency_1 
(trip_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/user/hbatra/frequency_1/';

INSERT OVERWRITE TABLE frequency_1 Select count(trip_id) from frequency;

Repeat the same for al rest 11 months.

Q5 Which community of Chicago has the maximum miles travelled by taxi in 2016?

Hdfs dfs –mkdir project2
Hdfs dfs –put *.csv project2

Vi summiles.pig

chicagoNew = load '/user/hbatra/project2/' USING PigStorage(',')
AS 
(taxi_id:Int, 
trip_start_timestamp:chararray, 
trip_end_timestamp:chararray, 
trip_seconds:Int, 
trip_miles:float, 
pickup_census_tract:Int, 
dropoff_census_tract:Int, 
pickup_community_area:Int, 
dropoff_community_area:Int, 
fare:float, 
tips:float, 
tolls:float, 
extras:float, 
trip_total:float, 
payment_type:chararray, 
company:Int, 
pickup_latitude:Int, 
pickup_longitude:Int, 
dropoff_latitude:Int, 
dropoff_longitude:Int);

describe chicagoNew ;

grpmiles = GROUP chicagoNew by pickup_community_area;

summiles = FOREACH grpmiles GENERATE group as pickup_community_area, SUM(chicagoNew.trip_miles) as sum_tripmiles;

STORE summiles INTO '/user/hbatra/output/Summiles_community' USING PigStorage(',');

Pig summiles.pig

Q6 Which communities in Chicago have the highest demand for pick-ups and Drop-offs?

DROP TABLE IF EXISTS area;
CREATE EXTERNAL TABLE IF NOT EXISTS area(taxi_id int, 
trip_start_timestamp timestamp, 
trip_end_timestamp timestamp, 
trip_seconds Int, 
trip_miles float, 
pickup_census_tract Int, 
dropoff_census_tract Int, 
pickup_community_area Int, 
dropoff_community_area Int, 
fare float, 
tips float, 
tolls float, 
extras float, 
trip_total float, 
payment_type String, 
company Int, 
pickup_latitude Int, 
pickup_longitude Int, 
dropoff_latitude Int, 
dropoff_longitude Int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/hbatra/chicago';


CREATE TABLE IF NOT EXISTS Pick_Drop 
as select pickup_community_area, count(pickup_community_area ) as Total_Community_Count, dropoff_community_area, count(dropoff_community_area ) as Total_Dropoff_Count from area 
GROUP BY pickup_community_area ,dropoff_community_area 
ORDER BY Total_Community_Count DESC LIMIT 20;

Q7 Which hour of the day and which day of the month has most pickups in New York?
hdfd dfs -mkdir project

drop table if exists NewYork_counts_travels;

CREATE EXTERNAL TABLE IF NOT EXISTS NewYork_counts(vendor_id String, 
pickup_datetime timestamp, 
dropoff_datetime timestamp, 
passenger_count int, 
trip_distance float, 
pickup_longitude float, 
pickup_latitude float, 
rate_code Int, 
store_and_fwd_flag String, 
dropoff_longitude float, 
dropoff_latitude float, 
payment_type String, 
fare_amount float, 
surcharge float, 
mta_tax float, 
tip_amount float, 
tolls_amount float, 
total_amount float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/mahisiri/NewYork/'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT COUNT(*) FROM NewYork_counts_travels;

drop table if exists NY_passenger_pickup_counts;

CREATE TABLE IF NOT EXISTS NY_passenger_pickup_counts 
AS SELECT pickup_datetime,
COUNT(DISTINCT(pickup_longitude)) AS PLttd,
COUNT(DISTINCT(pickup_latitude)) AS PLngttd
FROM NewYork_counts GROUP BY pickup_datetime;



Q8 What are the regions with highest demand and longest distance travelled in New York?


drop table if exists NewYork_counts_travels;

CREATE EXTERNAL TABLE IF NOT EXISTS NewYork_counts_travels(vendor_id String, 
pickup_datetime timestamp, 
dropoff_datetime timestamp, 
passenger_count int, 
trip_distance float, 
pickup_longitude float, 
pickup_latitude float, 
rate_code Int, 
store_and_fwd_flag String, 
dropoff_longitude float, 
dropoff_latitude float, 
payment_type String, 
fare_amount float, 
surcharge float, 
mta_tax float, 
tip_amount float, 
tolls_amount float, 
total_amount float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/mahisiri/NewYork/'
TBLPROPERTIES ('skip.header.line.count'='1');

drop table if exists NY_Distance_travelled_most_main;

CREATE TABLE IF NOT EXISTS  NY_Distance_travelled_most_main 
AS SELECT NewYork_counts_travels.pickup_longitude, NewYork_counts_travels.pickup_latitude, 
MAX(NewYork_counts_travels.trip_distance) AS longest 
FROM NewYork_counts_travels GROUP BY NewYork_counts_travels.pickup_longitude, NewYork_counts_travels.pickup_latitude 
ORDER BY longest DESC;



CREATE TABLE IF NOT EXISTS Pick_Drop
as
select pickup_community_area, count(pickup_community_area ) as Total_Community_Count, dropoff_community_area, count(dropoff_community_area ) as Total_Dropoff_Count
from area
GROUP BY pickup_community_area ,dropoff_community_area
ORDER BY Total_Community_Count DESC LIMIT 50;


hdfd dfs -mkdir project

HIVE
hdfd dfs -

DROP TABLE IF EXISTS area;

CREATE EXTERNAL TABLE IF NOT EXISTS area(taxi_id int, 
trip_start_timestamp timestamp, 
trip_end_timestamp timestamp, 
trip_seconds Int, 
trip_miles float, 
pickup_census_tract Int, 
dropoff_census_tract Int, 
pickup_community_area Int, 
dropoff_community_area Int, 
fare float, 
tips float, 
tolls float, 
extras float, 
trip_total float, 
payment_type String, 
company Int, 
pickup_latitude Int, 
pickup_longitude Int, 
dropoff_latitude Int, 
dropoff_longitude Int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/hbatra09/project';

CREATE TABLE IF NOT EXISTS Pick_Drop
as
select pickup_community_area, count(pickup_community_area ) as Total_Community_Count, dropoff_community_area, count(dropoff_community_area ) as Total_Dropoff_Count
from area
GROUP BY pickup_community_area ,dropoff_community_area
ORDER BY Total_Community_Count DESC LIMIT 50;






Q9 Which is the highest earning vendor in New York?

pig

NwYrk = LOAD '/user/mahisiri/NewYork/nyc_taxi_data_2014.csv' USING PigStorage(',')
AS (vendor_id:chararray, 
pickup_datetime:chararray, 
dropoff_datetime:chararray, 
passenger_count:int, 
trip_distance:long, 
pickup_longitude:double, 
pickup_latitude:double, 
rate_code:int, 
store_and_fwd_flag:chararray, 
dropoff_longitude:double, 
dropoff_latitude:double, 
payment_type:chararray, 
fare_amount:long, 
surcharge:long, 
mta_tax:long, 
tip_amount:long, 
tolls_amount:long, 
total_amount:long);

DESCRIBE NwYrk;

NwYrk_subset_1 = LIMIT NwYrk 100;

DESCRIBE NwYrk_subset_1;

DUMP NwYrk_subset_1;

by_vendorid_travels = GROUP NwYrk BY (vendor_id, total_amount);

by_vendorid_travels_counts = FOREACH by_vendorid_travels GENERATE
    FLATTEN(group) AS (vendor_id, total_amount), COUNT(NwYrk) AS vendor_travel_count;
    
STORE by_vendorid_travels_counts INTO 'output/vendorsWithMostTravels' USING PigStorage(',');


