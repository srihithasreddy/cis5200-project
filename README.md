# cis5200-project
Data Science Project

HIVE


Q1. HOW MANY PICKUPS IN NEW YORK? WHICH HOUR OF THE DAY IS THE MOST BUSIEST? WHCIH DAY OF A MONTH HAS MOST PICKUPS?

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

Q2 REGIONS WITH HIGHEST DEMAND AND LONGEST DISTANCES TRAVELLED IN NEW YORK?

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

PIG

Q3  VENDORS WITH HIGHEST DEMAND AND INCOME?

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

Q4 : What is the community with hghest number of pickup and dropoff filtered by Top 5?

hdfd dfs -mkdir project

HIVE

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

Q4 : What is the taxi id which generates maximum income and their total number of rides, filter by top 10 income?

create table if not exists taxi
as
select taxi_id, count(taxi_id) as cnt, pickup_community_area, count(pickup_community_area) as total_pickup, dropoff_community_area, count(dropoff_community_area ) as dropoff, SUM(trip_total) as sum
from area
group by taxi_id, pickup_community_area, dropoff_community_area
order by sum desc limit 20;

Q6 :  Which company earned the most in 2016 and what was the highest tip paid to that company?

CREATE TABLE IF NOT EXISTS Company2
as
select company, sum(trip_total) as Total_F, MAX(tips)
from area
Group by company
ORDER BY Total_F DESC LIMIT 50;

Q7 : What is the total fare for taxi services which was disputed for each month in Chicago? Comparison between the disputed amount and the total amount.

Hdfs dfs –mkdir project1

DROP TABLE IF EXISTS one_1;

CREATE EXTERNAL TABLE IF NOT EXISTS one_1(trip_id int, 
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

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_01.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_2;
create external table one_2 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_2/';

INSERT OVERWRITE TABLE one_2 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type,order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_02.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_3;
create external table one_3 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_3/';
INSERT OVERWRITE TABLE one_3 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type,order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_03.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_4;
create external table one_4 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_4/';
INSERT OVERWRITE TABLE one_4 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_04.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_5;
create external table one_5 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_5/';
INSERT OVERWRITE TABLE one_5 
Select payment_type, sum(fare) as Total_fare 
from one_1 
group by payment_type 
order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_05.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_6;
create external table one_6 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_6/';
INSERT OVERWRITE TABLE one_6 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_06.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_7;
create external table one_7 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_7/';
INSERT OVERWRITE TABLE one_7 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_07.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_8;
create external table one_8 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_8/';
INSERT OVERWRITE TABLE one_8 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_08.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_9;
create external table one_9 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_9/';
INSERT OVERWRITE TABLE one_9 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_09.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_10;
create external table one_10 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_10/';
INSERT OVERWRITE TABLE one_10 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_10.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_11;
create external table one_11 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_11/';
INSERT OVERWRITE TABLE one_11 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_11.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_12;
create external table one_12 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_12/';
INSERT OVERWRITE TABLE one_12 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_12.csv’ OVERWRITE INTO TABLE one_1;
Drop table if exists one_13;
create external table one_13 (payment_type String,fare float)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/nshrist/one_13/';
INSERT OVERWRITE TABLE one_13 Select payment_type,sum(fare) as Total_fare from one_1 group by payment_type order by Total_fare;

Q8 What is the total number of pick-ups for each month in Chicago(2016)?

LOAD DATA INPATH  '/user/nshrist/project/chicago_taxi_trips_2016_01.csv’ OVERWRITE INTO TABLE one_1;

Drop table if exists demand_1;

create external table demand_1 (trip_id int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/nshrist/demand_1/';

INSERT OVERWRITE TABLE demand_1 Select count(trip_id) from one_1;

Repeat for all the 12 months…



    
    


