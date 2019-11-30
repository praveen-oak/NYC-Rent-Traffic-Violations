drop table IF EXISTS ppo208.final_data;
create table IF NOT EXISTS ppo208.final_data (
trip_distance FLOAT,
pickup_location INT,
dropoff_location INT,
payment_type INT,
fare_amount FLOAT,
surcharge FLOAT,
tip_amount FLOAT,
tolls_amount FLOAT,
total_amount FLOAT,
pickup_rent FLOAT,
dropoff_rent FLOAT,
pickup_decile INT,
dropoff_decile INT,
tip_percent FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION "hdfs:///user/ppo208/project/impala/result";
