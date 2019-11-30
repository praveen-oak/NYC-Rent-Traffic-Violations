This folder contains all the hadoop map reduce jobs required to run on the final dataset before impala scripts can be used to analyse and extract useful information from the data.

It contains 2 Hadoop Mapreduce jobs and two shell scripts to build and run those jobs.

1. MergeData.java - This mapreduce job is used to take each row of the data in the taxi dataset and get its locality name using the map provided by NYC TLC open data. With this locality name, we can then lookup the corresponding median rent for the neighbourhood from the rent dataset. The rent for the pickup and dropoff location is then added to each row and output.


2. AddRentDeciles.java - This mapreduce job is used to classify each of row of the taxi dataset into one of the 10 deciles based on where the rent falls in the decile array. The deciles for both pickup and dropoff rent are calculated using a spark job and the input is fed into this map reduce job.

