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
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String u = itr.nextToken().toString();
                String v = itr.nextToken().toString();
                if(!u.equals(v)){
                    if(u.compareTo(v) < 0) word.set(u + "#" + v);
                    else word.set(v + "#" + u);
                    context.write(word, one);
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
        static ArrayList<String> edgeSet = new ArrayList<String>();
        private final static Text found = new Text("1");
        private final static Text need = new Text("0");

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            String[] split = key.toString().split("#");
            if(split[0].compareTo(split[1]) > 0) return;
            word.set(split[0]);
            if((!CurrentItem.equals(word)) && (!CurrentItem.equals("*"))){
                myOutPut(context);
                edgeSet = new ArrayList<String>();
            }
            CurrentItem = new Text(word);
            edgeSet.add(split[1]);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            myOutPut(context);
        }

        private static void myOutPut(Context reducerContext) throws IOException, InterruptedException {
            for(int i = 0; i < edgeSet.size(); ++i){
                String u = edgeSet.get(i);
                reducerContext.write(new Text(CurrentItem + "#" + u), found);
                for(int j = (i + 1); j < edgeSet.size(); ++j){
                    String v = edgeSet.get(j);
                    if(u.compareTo(v) > 0) continue;
                    reducerContext.write(new Text(u + "#" + v), need);
                }
            }
        }
    }

    public static void main(String inputPath, String outBufPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Dereplication");
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
