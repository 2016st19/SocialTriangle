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

public class Dereplication {
    public static class DereplicationMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String u = itr.nextToken().toString();
                String v = itr.nextToken().toString();
                if(!u.equals(v)){
                    context.write(new Text(u + "#" + v), one);
                    context.write(new Text(v + "#" + u), one);
                }
            }
        }
    }

    public static class DereplicationCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, one);
        }
    }

    public static class DereplicationNewPartitioner
            extends HashPartitioner<Text, IntWritable>{
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String term;
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class DereplicationReducer
            extends Reducer<Text, IntWritable, Text, Text>{
        private Text word = new Text();
        static Text CurrentItem = new Text("*");
        static StringBuilder myout = new StringBuilder();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            String[] split = key.toString().split("#");
            word.set(split[0]);
            if((!CurrentItem.equals(word)) && (!CurrentItem.equals("*"))){
                myOutPut(context);
                myout = new StringBuilder();
            }
            CurrentItem = new Text(word);
            myout.append(split[1] + "#");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            myOutPut(context);
        }

        private static void myOutPut(Context reducerContext) throws IOException, InterruptedException {
            if(myout.length() > 0){
                myout.setLength(myout.length() - 1);
                reducerContext.write(CurrentItem, new Text(myout.toString()));
            }
        }
    }

    public static void main(String inputPath, String outBufPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Dereplication");
        job.setNumReduceTasks(20);
        job.setJarByClass(Dereplication.class);
        job.setMapperClass(DereplicationMapper.class);
        job.setCombinerClass(DereplicationCombiner.class);
        job.setPartitionerClass(DereplicationNewPartitioner.class);
        job.setReducerClass(DereplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outBufPath));
        int exit = job.waitForCompletion(true) ? 0 : 1;
    }
}
