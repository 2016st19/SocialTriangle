/**
 * Created by 2016st19 on 11/21/16.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CollectCount {
    public static class CollectCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String k = itr.nextToken().toString();
                String v = itr.nextToken().toString();
                context.write(new Text("1"), new IntWritable(Integer.valueOf(v)));
            }
        }
    }

    public static class CollectCountCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(new Text("1"), new IntWritable(sum));
        }
    }

    public static class CollectCountReducer
            extends Reducer<Text, IntWritable, Text, Text>{
        private static int result = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            for(IntWritable v : values){
                result += v.get();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new Text("Total Triangle is:"), new Text(Integer.toString(result)));
        }
    }

    public static void main(String inputPath, String outBufPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CollectCount");
        job.setJarByClass(CollectCount.class);
        job.setCombinerClass(CollectCountCombiner.class);
        job.setMapperClass(CollectCountMapper.class);
        job.setReducerClass(CollectCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outBufPath));
        int exit = job.waitForCompletion(true) ? 0 : 1;
    }
}
