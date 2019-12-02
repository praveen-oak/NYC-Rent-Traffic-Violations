hdfs dfs -rm -r SparkData
hdfs dfs -mkdir SparkData
hdfs dfs -put ../Impala/manhattan.csv SparkData
hdfs dfs -put ../Impala/bronx.csv SparkData
hdfs dfs -put ../Impala/brooklyn.csv SparkData
hdfs dfs -put ../Impala/queens.csv SparkData
hdfs dfs -put ../Impala/staten_island.csv SparkData
spark-submit manhattan.py > manhattan.txt
spark-submit bronx.py > bronx.txt
spark-submit brooklyn.py > brooklyn.txt
spark-submit queens.py > queens.txt
spark-submit staten_island.py > staten_island.txt
