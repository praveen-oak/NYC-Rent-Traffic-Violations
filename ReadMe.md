
Project Structure:
load_data.py --> Python code that pulls data from public datasource and loads it into hdfs
clean_data.py --> Python script that calls pig script used to clean and project data
data_clean.pig --> pig script used to clean and project the columns to get all the data required for the project
constants.py --> Contains constant values used throughout the project

Data schema:
Input schema received from data source

vendor_id:chararray,
pickup_datetime:chararray,
dropoff_datetime:chararray,
passenger_count:int,
trip_distance:float,
pickup_longitude:float,
pickup_latitude:float,
rate_code:float,
store_and_fwd_flag: chararray,
dropoff_longitude:float,
dropoff_latitude:float,
payment_type:chararray,
fare_amount:float,
surcharge:float,
mta_tax:float,
tip_amount:float,
tolls_amount:float,
total_amount:float


Data schema used for project:

trip_distance:float,
pickup_longitude:float,
pickup_latitude:float,
dropoff_longitude:float,
dropoff_latitude:float,
payment_type:chararray,
surcharge:float,
tip_amount:float,
tolls_amount:float,
total_amount:float