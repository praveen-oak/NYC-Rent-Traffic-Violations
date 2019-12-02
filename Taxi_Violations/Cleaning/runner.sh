hdfs dfs -rm -r TaxiData
hdfs dfs -mkdir TaxiData
rm -rf /scratch/ak7380/TaxiData
mkdir /scratch/ak7380/TaxiData
hdfs dfs -rm -r Violations
hdfs dfs -mkdir Violations
rm -rf /scratch/ak7380/Violations
mkdir /scratch/ak7380/Violations
python load_data.py
hdfs dfs -rm -r TaxiDataOutput
hdfs dfs -rm -r ViolationsOutput
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -files Taxi/taxi_cleaning_mapper.py,Taxi/taxi_cleaning_reducer.py,Taxi/taxi+_zone_lookup.csv -mapper "python taxi_cleaning_mapper.py" -reducer "python taxi_cleaning_reducer.py" -input TaxiData -output TaxiDataOutput
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -files Violations/violations_cleaning_mapper.py,Violations/violations_cleaning_reducer.py -mapper "python violations_cleaning_mapper.py" -reducer "python violations_cleaning_reducer.py" -input Violations -output ViolationsOutput
