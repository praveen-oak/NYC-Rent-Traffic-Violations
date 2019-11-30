rm -rf *.jar *.class
javac  -classpath `yarn classpath`:. -d . MergeData.java
jar -cvf merge.jar *.class