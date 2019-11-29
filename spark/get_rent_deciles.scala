import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val month = args(1)
val year = args(0)
val input_path = "/user/ppo208/project/final_data/"+year+"_"+month+".csv"
var df = spark.read.format("csv").option("dropInvalid", true).load("/user/ppo208/project/final_data.csv").toDF("trip_distance", "pickup_location",
	"dropoff_location", "payment_type", "fare_amount", "surcharge", "tip_amount", "tolls_amount", "total_amount", "pickup_rent", "dropoff_rent").limit(100000)

df = df.withColumn("trip_distance", 'trip_distance cast "float")
df = df.withColumn("fare_amount", 'fare_amount cast "float")
df = df.withColumn("surcharge", 'surcharge cast "float")
df = df.withColumn("tip_amount", 'tip_amount cast "float")
df = df.withColumn("tolls_amount", 'tolls_amount cast "float")
df = df.withColumn("pickup_rent", 'pickup_rent cast "float")
df = df.withColumn("dropoff_rent", 'dropoff_rent cast "float")
df = df.withColumn("payment_type", 'payment_type cast "int")
val dropoff_with_payment = df.select("dropoff_rent", "pickup_rent")
val dropoff_rent_deciles = dropoff_with_payment.stat.approxQuantile("dropoff_rent",Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),0.0).distinct;
val pickup_rent_deciles = dropoff_with_payment.stat.approxQuantile("pickup_rent",Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),0.0).distinct;