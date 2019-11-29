rm -rf *.jar *.class
javac  -classpath `yarn classpath`:. -d . AddRentDeciles.java
jar -cvf addRent.jar *.class
hdfs dfs -rmr /user/ppo208/project/impala/result/
hadoop jar addRent.jar AddRentDeciles /user/ppo208/project/impala/final_data.csv /user/ppo208/project/impala/result/