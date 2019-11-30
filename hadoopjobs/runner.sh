rm -rf *.jar *.class
javac  -classpath `yarn classpath`:. -d . MergeData.java
jar -cvf merge.jar *.class
hdfs dfs -rmr /user/ppo208/project/impala/result/
hadoop jar merge.jar MergeData /user/ppo208/project/impala/final_data.csv /user/ppo208/project/impala/result/