
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class RandomForestRegression {

    public static void main(String args[]){

        SparkSession spark = SparkSession
                .builder()
                .appName("Random Forest regression")
                .getOrCreate();

        StructType schema = new StructType()
                .add("tripDistance", "float")
                .add("pickupLocationId", "int")
                .add("dropoffLocationId", "int")
                .add("paymentType", "int")
                .add("fareAmount", "float")
                .add("surcharge", "float")
                .add("tipAmount", "float")
                .add("tollsAmount", "float")
                .add("totalAmount", "float")
                .add("pickupRent", "float")
                .add("dropoffRent", "float");


        Dataset<Row> df = spark.read().csv("hdfs://"+args[0]);


        Dataset<Row>[] splits = df.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        LinearRegressionModel lrModel = lr.fit(trainingData);
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show();
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());
    }
}
