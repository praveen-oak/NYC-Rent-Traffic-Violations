import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

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

public class MergeData {
    private static final String ZONE_DATA_LOCATION_KEY = "zone_location";
    private static final String  MEDIAN_RENT_LOCATION_KEY = "median_rent";
    private static final String INDEX = "index";

    public static class MergeDataMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int index = Integer.parseInt(conf.get(INDEX));
            Map<String, String> zoneMap = getZoneMap(conf.get(ZONE_DATA_LOCATION_KEY));
            Map<String, String[]> medianRentMap = getMedianRent(conf.get(MEDIAN_RENT_LOCATION_KEY));
            String[] tuples = value.toString().split(",");
            if(tuples.length == 9){
                String pickupLocationId = tuples[1];
                String dropoffLocationId = tuples[2];
                Float pickupLocationRent = getMedianRentFromLocationCode(zoneMap, medianRentMap, index, pickupLocationId);
                Float dropoffLocationRent = getMedianRentFromLocationCode(zoneMap, medianRentMap, index, dropoffLocationId);

                if(pickupLocationRent != null && dropoffLocationRent != null){
                    context.write(new Text(key.toString()), new Text(value.toString()+","+pickupLocationRent+","+dropoffLocationRent));
                }
            }
        }

        private Map<String, String[]> getMedianRent(String filePath) throws IOException {
            Map<String, String[]> medianRentMap = new HashMap<String, String[]>();
            BufferedReader br=new BufferedReader(new FileReader(filePath));
            String line;
            line=br.readLine();
            while (line != null) {
                String tuples[] = line.split(",");
                if(tuples.length >= 1){
                    medianRentMap.put(tuples[0].trim(), tuples);
                }
                line = br.readLine();
            }
            return medianRentMap;
        }


        private Map<String, String> getZoneMap(String filePath) throws IOException {
            Map<String, String> zoneMap = new HashMap<String, String>();

            BufferedReader br=new BufferedReader(new FileReader(filePath));
            String line;
            line=br.readLine();
            while (line != null) {
                String tuples[] = line.split(",");
                if(tuples.length >= 3){
                    zoneMap.put(tuples[0], tuples[2]);
                }

                line = br.readLine();
            }
            return zoneMap;
        }

        private Float getMedianRentFromLocationCode(Map<String, String> zoneFilePath, Map<String, String[]> medianRent, int index, String locationId){
            //first get location string from location id
            if(!zoneFilePath.containsKey(locationId.trim())) {
                return null;
            }
            String locationName = zoneFilePath.get(locationId);
            String[] rentData = medianRent.get(locationName);
            if(rentData == null){
                return null;
            }
            int realIndex = index;
            if(rentData.length <= realIndex){
                return null;
            }
            if(rentData[realIndex] == ""){
                return null;
            }
            try{
                return Float.parseFloat(rentData[realIndex]);
            }catch(NumberFormatException e){
                return null;
            }
        }
    }

    public static class MergeDataReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
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
        conf.set(MEDIAN_RENT_LOCATION_KEY, args[2]);
        conf.set(ZONE_DATA_LOCATION_KEY, args[3]);
        conf.set(INDEX, args[4]);
        Job job = Job.getInstance(conf, "Merge data from neighbourhood and zone files into taxi data");
        job.setJarByClass(MergeData.class);
        job.setMapperClass(MergeDataMapper.class);
        job.setReducerClass(MergeDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}