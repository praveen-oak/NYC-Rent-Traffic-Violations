# Analysing New York Taxi Data to Determine Affluence and Road Safety of New York Neighbourhoods

Team Members -
* Praveen Oak
* Alankrith Krishnan

Project is available on github. Access is private, but I can grant based on request.
https://github.com/praveen-oak/rtbd

## Project Structure

### Taxi Data and Monthly Rent
* etl - Contains all code related to fetching, loading, and cleaning data
	* `load_data.py` --> Python code that pulls data from public datasource and loads it into hdfs
	* `clean_data.py` --> Python script that calls pig script used to clean and project data
	* `data_clean.pig` --> pig script used to clean and project the columns to get all the data 	required for the project
	* `levenstein.py` --> Python script that matches locality in median_rent.csv with zone_lookup.csv
	* `merge_data.py` --> Takes individual data files after clean_data and merges them into a single dataset
	* `constants.py` --> Contains constant values used throughout the project


* Hadoop Jobs - Contains all code for Mapreduce based java programs
	* AddRentDeciles.java - Job that adds pickup and dropoff rent deciles to each row
	* MergeData.java - Adds pickup and dropoff rent to each row based on the mapping between
		the zone in the taxi dataset and locality in the rent dataset.

* impala - Contains impala scripts to load and analyse data

* profiling - Pig scripts for profiling

* spark - Spark script to generate rent deciles for merged taxi dataset.

* results - .csv files of results, which are manually created after looking at data from impala scripts and formatting them

All data for this section is present in

/user/ppo208/project/

Downloaded data from internet - /user/ppo208/project/raw_data

After cleaning - /user/ppo208/project/clean_data

After merging individual years - /user/ppo208/project/merge_data

After merging all years and running mapreduce jobs - /user/ppo208/project/impala/final_data.csv

Impala loading and results - /user/ppo208/project/impala

### Taxi Data and Traffic Violations

* In the Taxi_Violations folder

* Cleaning
	* load_data.py -> Loads data to HDFS
	* Taxi/taxi_cleaning_mapper.py -> Mapper to clean Taxi Data
	* Taxi/taxi_cleaning_reducer.py -> Reducer to clean Taxi Data
	* Taxi/taxi+_zone_lookup.csv -> Lookup table for neighbourhood codes
	* Violations/violations_cleaning_mapper.py -> Mapper to clean Violations Data
	* Violations/violations_cleaning_reducer.py -> Reducer to clean Violations Data

* Impala
	* setup.sql -> Setup Taxi and Violations tables from HDFS
	* bronx.sql -> Impala script to run analysis and exporting to csv for bronx
	* brooklyn.sql -> Impala script to run analysis and exporting to csv for brooklyn
	* manhattan.sql -> Impala script to run analysis and exporting to csv for manhattan
	* queens.sql -> Impala script to run analysis and exporting to csv for queens
	* staten_island.sql -> Impala script to run analysis and exporting to csv for staten island
	* visualization.py -> Visualization python script to normalize and plot data
	* *.csv -> Corresponding csv files for output
	* *.png -> Corresponding png files for visualization output

* Spark
	* bronx.py -> Spark script to calculate correlations and output for bronx
	* brooklyn.py -> Spark script to calculate correlations and output for brooklyn
	* manhattan.py -> Spark script to calculate correlations and output for manhattan
	* queens.py -> Spark script to calculate correlations and output for queens
	* staten_island.py -> Spark script to calculate correlations and output for staten island
	* *.txt -> Corresponding text file outputs for each borough

All data for this section is present in

/user/ak7380/

/scratch/ak7380/ -> Final Output

Taxi Raw Data - /user/ak7380/TaxiData

Taxi Cleaned Data - /user/ak7380/TaxiDataOutput

Violations Raw Data - /user/ak7380/Violations

Violations Cleaned Data - /user/ak7380/ViolationsOutput

Final Impala Data used for Spark - /user/ak7380/SparkData

Final Correlation Outputs - /scratch/ak7380/

## Running the project

### Taxi Data and Monthly Rent

* Load, clean and merge data
```
python load_data.py
python clean_data.py
python merge_data.py
```

```
hdfs dfs -cat /user/ppo208/project/merge_data/* | hadoop fs -put - /user/ppo208/project/final_data.csv
```

* Run Mapreduce jobs(from hadoopjobs)
```
./runner.sh
```
Get rent decile from spark job in spark folder
```
spark2-shell --master local -i get_rent_deciles.scala --conf spark.driver.args="/user/ppo208/project/final_data.csv"
```

Add these deciles to AddRentDeciles.java in hadoopjobs folder, and run that mapreduce job
```
./add_rent_deciles_runner.sh
```

* Load data to imapala and analyse(imapala folder)
Load the data into impala
```
impala-shell --query_file=load_impala_table.sql -i compute-1-1:21000
```
Run the analytics commands
```
./runner.sh
```

* All output files are saved in the impala folder

### Taxi Data and Traffic Violations

* Go to the corresponding directory (Cleaning, Impala, Spark) and run `runner.sh`
* Make sure to provide permissions first with `chmod +x runner.sh` (or check the README in each folder alternatively for the same)

## Data Schema

### Taxi Data and Monthly Rent

* Input schema

```
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
```

* Data schema used

```
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
pickup_rent:float,
dropoff_rent:float,
pickup_decile:int,
dropoff_decile:int
```

### Taxi Data and Traffic Violations

* Cleaned Data Schema

	* Taxi Data
	```
	date:string,
	borough:string
	```

	* Traffic Violations Data
	```
	date:string,
	borough:string
	```

* Final Schema (used for Spark, for each borough)
	```
	date:string,
	borough:string,
	tcount:bigint,
	vcount bigint
	```
