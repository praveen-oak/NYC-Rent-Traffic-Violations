import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

var df = spark.read.format("csv").option("dropInvalid", true).load("/user/ppo208/project/final_data/2018_12.csv").toDF("trip_distance", "pickup_location", "dropoff_location", "payment_type", "fare_amount", "surcharge", "tip_amount", "tolls_amount", "total_amount", "pickup_rent", "dropoff_rent")

df = df.withColumn("trip_distance", 'trip_distance cast "float")
df = df.withColumn("fare_amount", 'fare_amount cast "float")
df = df.withColumn("surcharge", 'surcharge cast "float")
df = df.withColumn("tip_amount", 'tip_amount cast "float")
df = df.withColumn("tolls_amount", 'tolls_amount cast "float")
df = df.withColumn("pickup_rent", 'pickup_rent cast "float")
df = df.withColumn("dropoff_rent", 'dropoff_rent cast "float")
val dropoff_tip_df = df.select("dropoff_rent", "tip_amount", "fare_amount").filter(df.col("tip_amount").isNotNull).where("fare_amount > 5.0")
val dropoff_with_percent_df = dropoff_tip_df.withColumn("tip_percent", dropoff_tip_df("tip_amount") / dropoff_tip_df("fare_amount")).where("tip_percent < 70.0")

val assembler = new VectorAssembler().setInputCols(Array("tip_percent")).setOutputCol("features")
val gbt = new GBTRegressor().setLabelCol("dropoff_rent").setFeaturesCol("features").setMaxIter(10)
val assembled_df = assembler.transform(dropoff_with_percent_df.na.drop)
val Array(trainingData, testData) = assembled_df.randomSplit(Array(0.8, 0.2))

val gbtModel = gbt.fit(trainingData)
val predictions = gbtModel.transform(testData)
val evaluator = new RegressionEvaluator().setLabelCol("dropoff_rent").setPredictionCol("prediction").setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)