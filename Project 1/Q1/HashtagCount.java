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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HashtagCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String wordString = word.toString();
//added
// checks if the word is a hashtag
                if (wordString.matches("^#[\\w\\W\\d_-]+$")) {
                    context.write(new Text(word.toString().substring(1)), one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        private TreeMap<Integer, String> maxMap = new TreeMap<Integer, String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
//added
//adds the count of the hastag to a sorted map
            maxMap.put(sum, key.toString());
//only keeps the top 25 results
            if (maxMap.size() > 25) {
                maxMap.remove(maxMap.firstKey());
            }
        }
//takes the top 25 stored in maxmap and writes to a file
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
        Job job = Job.getInstance(conf, "hashtag count");
        job.setJarByClass(HashtagCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}