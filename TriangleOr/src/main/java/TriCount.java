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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TriCount {
    public static class TriCountMapper
            extends Mapper<Object, Text, Text, Text>{
        private static final String found = "1";
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String k = itr.nextToken().toString();
                String v = itr.nextToken().toString();
                word.set(k);
                if(v.equals(found)) context.write(word, new Text("#"));
                else context.write(word, new Text("1"));
            }
        }
    }

    public static class TriCountCombiner
            extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            boolean isExist = false;
            for (Text val : values) {
                if(val.toString().equals("#")) isExist = true;
                else sum += Integer.valueOf(val.toString());
            }
            if(isExist) context.write(key, new Text("#"));
            if(sum > 0) context.write(key, new Text(Integer.toString(sum)));
        }
    }

    public static class TriCountNewPartitioner
            extends HashPartitioner<Text, Text>{
        public int getPartition(Text key, Text value, int numReduceTasks){
            return super.getPartition(key, value, numReduceTasks);
        }
    }

    public static class TriCountReducer
            extends Reducer<Text, Text, Text, Text>{
        private Text word = new Text();
        static Text CurrentItem = new Text("*");
        private static int result = 0;
        private static int bufRes = 0;
        private static boolean isExist = false;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            word.set(key);
            if((!CurrentItem.equals(word)) && (!CurrentItem.equals("*"))){
                myTreat();
                bufRes = 0;
                isExist = false;
            }
            CurrentItem = new Text(word);
            for(Text v : values){
                if(v.toString().equals("#")) isExist = true;
                else bufRes += Integer.valueOf(v.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            myTreat();
            context.write(new Text("Total:"), new Text(Integer.toString(result)));
        }

        private static void myTreat(){
            if(isExist) result += bufRes;
        }
    }

    public static void main(String inputPath, String outBufPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TriCount");
        job.setNumReduceTasks(20);
        job.setJarByClass(TriCount.class);
        job.setMapperClass(TriCountMapper.class);
        job.setCombinerClass(TriCountCombiner.class);
        job.setPartitionerClass(TriCountNewPartitioner.class);
        job.setReducerClass(TriCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outBufPath));
        int exit = job.waitForCompletion(true) ? 0 : 1;
    }
}
