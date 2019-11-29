import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val month = args(1)
val year = args(0)
val input_path = "/user/ppo208/project/final_data/"+year+"_"+month+".csv"
var df = spark.read.format("csv").option("dropInvalid", true).load("/user/ppo208/project/final_data.csv").toDF("trip_distance", "pickup_location",
	"dropoff_location", "payment_type", "fare_amount", "surcharge", "tip_amount", "tolls_amount", "total_amount", "pickup_rent", "dropoff_rent")

df = df.withColumn("trip_distance", 'trip_distance cast "float")
df = df.withColumn("fare_amount", 'fare_amount cast "float")
df = df.withColumn("surcharge", 'surcharge cast "float")
df = df.withColumn("tip_amount", 'tip_amount cast "float")
df = df.withColumn("tolls_amount", 'tolls_amount cast "float")
df = df.withColumn("pickup_rent", 'pickup_rent cast "float")
df = df.withColumn("dropoff_rent", 'dropoff_rent cast "float")
df = df.withColumn("payment_type", 'payment_type cast "int")
val dropoff_with_payment = df.select("dropoff_rent", "payment_type")
val dropoff_with_payment_card = dropoff_with_payment.where("payment_type = 1")
val rent_deciles = dropoff_with_payment.stat.approxQuantile("dropoff_rent",Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),0.0).distinct;

val bucketizer = new Bucketizer().setInputCol("dropoff_rent").setOutputCol("rent_decile").setSplits(rent_deciles)
val card_binned_df = bucketizer.transform(dropoff_with_payment_card)
val card_binned_results = card_binned_df.groupBy("rent_decile", "payment_type").agg(count("payment_type"))

val total_binned_df = bucketizer.transform(dropoff_with_payment)
val total_binned_results = total_binned_df.groupBy("rent_decile", "payment_type").agg(count("payment_type"))

card_binned_results.coalesce(1).write.mode("overwrite").csv("/user/ppo208/project/results_data/payment_type/card/")
total_binned_results.coalesce(1).write.mode("overwrite").csv("/user/ppo208/project/results_data/payment_type/total/")