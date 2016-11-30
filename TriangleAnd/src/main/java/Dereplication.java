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
        private final static IntWritable zero = new IntWritable(0);
        private Text word1 = new Text();
        private Text word2 = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String u = itr.nextToken().toString();
                String v = itr.nextToken().toString();
                if(!u.equals(v)){
                    word1.set(u + "#" + v);
                    word2.set(v + "#" + u);
                    context.write(word1, one);
                    context.write(word2, zero);
                }
            }
        }
    }

    public static class DereplicationCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            boolean one = false, zero =false;
            for(IntWritable v : values){
                if(one && zero) break;
                if((!one) && (v.get() == 1)){
                    one = true;
                    context.write(key, new IntWritable(1));
                }
                if((!zero) && (v.get() == 0)){
                    zero = true;
                    context.write(key, new IntWritable(0));
                }
            }
        }
    }

    public static class DereplicationNewPartitioner
            extends HashPartitioner<Text, IntWritable>{
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            return super.getPartition(key, value, numReduceTasks);
        }
    }

    public static class DereplicationReducer
            extends Reducer<Text, IntWritable, Text, Text>{
        private Text word = new Text();
        static Text CurrentItem = new Text("*");
        private static boolean positive = false, negative = false;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            String[] split = key.toString().split("#");
            if(split[0].compareTo(split[1]) < 0){
                word.set(key);
                if((!CurrentItem.equals(word)) && (!CurrentItem.equals("*"))){
                    myOutPut(context);
                    positive = negative = false;
                }
                CurrentItem = new Text(word);
                for(IntWritable v : values){
                    if((positive) && (negative)) break;
                    if((!positive) && (v.get() == 1)) positive = true;
                    if((!negative) && (v.get() == 0)) negative = true;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            myOutPut(context);
        }

        private static void myOutPut(Context reducerContext) throws IOException, InterruptedException {
            if((positive) && (negative)){
                String[] split = CurrentItem.toString().split("#");
                if(split[0].compareTo(split[1]) < 0) reducerContext.write(new Text(split[0]), new Text(split[1]));
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