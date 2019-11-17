rm -rf *.jar *.class
hdfs dfs -rmr /user/ppo208/project/merged_data
javac  -classpath `yarn classpath`:. -d . MergeData.java
jar -cvf merge.jar *.class
hadoop jar merge.jar MergeData /user/ppo208/project/clean_data/2018_01/ /user/ppo208/project/merge_data/2018_01 /home/ppo208/project/median_rent.csv /home/ppo208/project/zone_lookup_mapped.csv 1