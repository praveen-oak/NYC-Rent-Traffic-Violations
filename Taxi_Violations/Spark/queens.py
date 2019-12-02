from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import csv

sc = SparkContext()
sql = SQLContext(sc)

rdd = sc.textFile("SparkData/queens.csv")
rdd = rdd.mapPartitions(lambda x: csv.reader(x))
df = sql.createDataFrame(rdd, ['date', 'boro', 'tcount', 'vcount'])
df = df.withColumn('tcount', df['tcount'].cast("decimal"))
df = df.withColumn('vcount', df['vcount'].cast("decimal"))
print('Correlation: %f' % df.stat.corr('tcount', 'vcount'))
