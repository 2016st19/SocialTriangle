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

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String mykey = itr.nextToken().toString();
                String myvalue = itr.nextToken().toString();
                String[] split = myvalue.split("#");
                for(int i = 0; i < split.length; ++i){
                    String a = split[i];
                    if(a.compareTo(mykey) > 0) break;
                    StringBuilder myout = new StringBuilder();
                    for(int j = (i + 1); j < split.length; ++j){
                        String b = split[j];
                        if(mykey.compareTo(b) < 0) myout.append(mykey + b + "#");
                        else myout.append(b + mykey + "#");
                    }
                    if(myout.length() > 0){
                        myout.setLength(myout.length() - 1);
                        context.write(new Text(a), new Text(myout.toString()));
                    }
                }
            }
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
        private static int result = 0;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Set<String> triangle = new HashSet<String>(0);
            int count = 0;
            for(Text value : values){
                String[] nodes = value.toString().split("#");
                for(String str : nodes){
                    triangle.add(str);
                    count++;
                }
            }
            result += (count - triangle.size());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new Text("Total:"), new Text(Integer.toString(result)));
        }
    }

    public static void main(String inputPath, String outBufPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TriCount");
        job.setNumReduceTasks(20);
        job.setJarByClass(TriCount.class);
        job.setMapperClass(TriCountMapper.class);
        job.setPartitionerClass(TriCountNewPartitioner.class);
        job.setReducerClass(TriCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outBufPath));
        int exit = job.waitForCompletion(true) ? 0 : 1;
    }
}