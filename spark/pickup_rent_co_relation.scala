import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator


var df = spark.read.format("csv").option("dropInvalid", true).load("/user/ppo208/project/temp_merge/2018_01/")
df = spark.read.format("csv").option("dropInvalid", true).load("/user/ppo208/project/temp_merge/2018_01/").toDF("trip_distance", "pickup_location", "dropoff_location", "payment_type", "fare_amount", "surcharge", "tip_amount", "tolls_amount", "total_amount", "pickup_rent", "dropoff_rent")

df = df.withColumn("trip_distance", 'trip_distance cast "float")
df = df.withColumn("fare_amount", 'fare_amount cast "float")
df = df.withColumn("surcharge", 'surcharge cast "float")
df = df.withColumn("tip_amount", 'tip_amount cast "float")
df = df.withColumn("tolls_amount", 'tolls_amount cast "float")
df = df.withColumn("pickup_rent", 'pickup_rent cast "float")
df = df.withColumn("dropoff_rent", 'dropoff_rent cast "float")

val pickup_tip = df.select("pickup_rent", "tip_amount")
val assembler = new VectorAssembler().setInputCols(Array("tip_amount")).setOutputCol("features")
val data = assembler.transform(pickup_tip)
val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("pickup_rent").setFeaturesCol("features")
val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

val lrModel = lr.fit(trainingData)
val predictions = lrModel.transform(testData)
val evaluator = new RegressionEvaluator().setLabelCol("pickup_rent").setPredictionCol("prediction").setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)




