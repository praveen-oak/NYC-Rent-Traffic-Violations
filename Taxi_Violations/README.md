# Parking and Camera Violations vs Taxi Trip Data

## Basic Idea

### Parking and Camera Violations

* Filter by boroughs.
* Get the total number of violations, per month.

### Taxi Trip Data

* Get the Pickup and Dropoff Locations and get the boroughs from the zone lookup.
* Once you have the boroughs, aggregate by month for count.

### Comparison

* Convert the data to timeseries for both datasets and find correlations between number of taxi trips and number of violations.

## Parking and Camera Violations Cleaning (MapReduce)

```
NY, MN - Manhattan
BK, K - Brooklyn
BX - Bronx
Q, QN - Queens
ST - Staten Island
```

* Drop all columns except Issue Date and County Column
* Drop all rows with empty values in them.
* Change all dates to just yyyy-mm

## Taxi Trip Data Cleaning (MapReduce)

* Drop all columns except Pickup and Dropoff Location and Pickup and Dropoff Date Time
* Convert Date Time to yyyy-mm
* Map Taxi Zone Code to Boroughs between Manhattan, Brooklyn, Bronx, Queens, Staten Island.
* Add up the pickup and dropoff as a single column.

## Impala (Transformations and views exported to csv for easy correlation analysis)

* For each borough, group by the pickup date and find count for both datasets.
* Join the two datasets by their date and borough.
* For each borough, sort in ascending order by the date and return counts of both (taxi trips and violations).

## Spark (Comparison/Correlation)

* Load to Spark and find correlations between the two counts for each borough (over 2017-2018).
