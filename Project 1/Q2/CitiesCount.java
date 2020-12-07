import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

// two mappers one reducer as apposed to one mapper/reducer
public class CitiesCount {
// takes the input file with the tweet Id's and maps user Id's to the count
    public static class TweetIDMapper extends Mapper<Object, Text, Text, Text> {

        public Text keyEmit = new Text(); 
        public Text valueEmit = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split("\t");

            if (tokens.length < 1) return;

            String userID = tokens[0];
            // String tweetID = tokens[1];

            keyEmit.set(userID); // right here
            valueEmit.set("1-1"); //diferentiate which mapper the data is coming from in the reducer, everything is keyed on the user id
            context.write(keyEmit, valueEmit);
        }
    }
//user id to what city they are in
    public static class UserCityMapper extends Mapper<Object, Text, Text, Text> {

        public Text keyEmit = new Text();
        public Text valueEmit = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split("\t");

            if (tokens.length < 2) return;

            String userID = tokens[0];
            String city = tokens[1];

            keyEmit.set(userID); // key it on the same value
            valueEmit.set("2-" + city); // 2- means it comes from the city mapper
            context.write(keyEmit, valueEmit); //writes to the reducer
        }
    }
// single reducer that takes one value, 1-, 2-, to an integer. If it's 1, tweet mapper, 2 - city mapper
    public static class TweetCityReducer extends Reducer<Text,Text,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        private TreeMap<Integer, String> maxMap = new TreeMap<Integer, String>();
// goes through alkl the values and split it on the hyphen, 1 or 2
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            String city = "";
            for (Text val : values) {
                String ids[] = val.toString().split("-");
                int mapperID = Integer.parseInt(ids[0]);
//counts up the tweet, maps sum to the city
                if (mapperID == 1) { // <USER_ID, COUNT>
                    sum += 1;
                } else if (mapperID == 2) { // <USER_ID, CITY>
                    city = ids[1];
                }
            }

            maxMap.put(sum, city);

            if (maxMap.size() > 15) {
                maxMap.remove(maxMap.firstKey());
            }
        }
//removes anythin thats in the top 15 and writes it to file
        public void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<Integer, String> entry : maxMap.entrySet()) {
                int count = entry.getKey();
                String name = entry.getValue();

                context.write(new Text(name), new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "city count");

        job.setJarByClass(CitiesCount.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TweetIDMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserCityMapper.class);
        job.setReducerClass(TweetCityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}