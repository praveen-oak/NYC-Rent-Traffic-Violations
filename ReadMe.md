Project is available on github. Access is private, but I can grant based on request.
https://github.com/praveen-oak/rtbd


Project Structure:
1. etl - Contains all code related to fetching, loading, and cleaning data

	1.a load_data.py --> Python code that pulls data from public datasource and loads it into hdfs
	1.b clean_data.py --> Python script that calls pig script used to clean and project data
	1.c data_clean.pig --> pig script used to clean and project the columns to get all the data 	required for the project
	1.d levenstein.py --> Python script that matches locality in median_rent.csv with zone_lookup.csv
	1.e merge_data.py --> Takes individual data files after clean_data and merges them into a single dataset
	1.e constants.py --> Contains constant values used throughout the project


2. Hadoop Jobs - Contains all code for Mapreduce based java programs
	2.a AddRentDeciles.java - Job that adds pickup and dropoff rent deciles to each row
	2.b MergeData.java - Adds pickup and dropoff rent to each row based on the mapping between
		the zone in the taxi dataset and locality in the rent dataset.

3. imapala - Contains impala scripts to load and analyse data

4. profiling - Pig scripts for profiling 

5. spark - Spark script to generate rent deciles for merged taxi dataset.

6. results - .csv files of results, which are manually created after looking at data from 				imapala scripts and formatting them

All data for the project is present in 
/user/ppo208/project/

Downlaoded data from internet - /user/ppo208/project/raw_data
After cleaning - /user/ppo208/project/clean_data
After merging individual years - /user/ppo208/project/merge_data
After merging all years and running mapreduce jobs - /user/ppo208/project/impala/final_data.csv 
Impala loading and results - /user/ppo208/project/impala


Steps to run the project(from etl)
1. Load, clean and merge data
python load_data.py
python clean_data.py
python merge_data.py

hdfs dfs -cat /user/ppo208/project/merge_data/* | hadoop fs -put - /user/ppo208/project/final_data.csv 

2. Run Mapreduce jobs(from hadoopjobs)
./runner.sh
Get rent decile from spark job in spark folder
spark2-shell --master local -i get_rent_deciles.scala --conf spark.driver.args="/user/ppo208/project/final_data.csv"

Add these deciles to AddRentDeciles.java in hadoopjobs folder, and run that mapreduce job
./add_rent_deciles_runner.sh

3. Load data to imapala and analyse(imapala folder)
Load the data into impala
impala-shell --query_file=load_impala_table.sql -i compute-1-1:21000
#run the analytics commands
./runner.sh

4. All output files are saved in the imapala folder


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
total_amount:float,
pickup_rent: float,
dropoff_rent : float,
pickup_decile : int,
dropoff_decile : int.

