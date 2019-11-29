
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AddRentDeciles {
    private static final Float[] dropoffRentDeciles = {1400.0f, 2200.0f, 2600.0f, 2995.0f, 3000.0f, 3300.0f, 3450.0f, 3850.0f, 3970.0f, 7475.0f};
    private static final Float[] pickupRentDeciles = {1483.0f, 2200.0f, 3000.0f, 3350.0f, 3500.0f, 3650.0f, 3900.0f, 3978.0f, 4365.0f, 7800.0f};

    public static class AddRentDecileMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tuples = value.toString().split(",");
            if(tuples.length == 11){
                Float pickupRent = Float.valueOf(tuples[9]);
                Float dropoffRent = Float.valueOf(tuples[10]);
                Float fareAmount = Float.valueOf(tuples[4]);
                Float tipAmount = Float.valueOf(tuples[6]);
                Float tipPercent = tipAmount/fareAmount;
                if(pickupRent != null && dropoffRent != null){
                    Integer pickupDecile = binSearch(pickupRentDeciles, pickupRent);
                    Integer dropoffDecile = binSearch(dropoffRentDeciles, dropoffRent);
                    context.write(new Text(key.toString()), new Text(value.toString()+","+pickupDecile+","+dropoffDecile+","+tipPercent));
                }
            }
        }

        private int binSearch(Float[] values, float toFind){
            int left = 0;
            int right = values.length - 1;

            while(left <= right){
                if(toFind < values[left]){
                    return left;
                }
                if(toFind > values[right]){
                    return right;
                }
                if(left == right){
                    return left;
                }
                if(left == right - 1){
                    return left;
                }
                int mid = (left+right)/2;
                if(values[mid] == toFind){
                    return mid;
                }else if(toFind > values[mid]){
                    //find is to right of values
                    left = mid + 1;

                }else{
                    right = mid - 1;
                }
            }
            return left;
        }

    }

    public static class AddRentDecileReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Mapper.Context context
        ) throws IOException, InterruptedException {
            String finalString = "";
            for (Text val : values) {
                finalString += val.toString();
            }
            context.write(NullWritable.get(), new Text(finalString));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Add deciles to merged data");
        job.setJarByClass(AddRentDeciles.class);
        job.setMapperClass(AddRentDecileMapper.class);
        job.setReducerClass(AddRentDecileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
