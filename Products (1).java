import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Products extends Configured implements Tool{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text item = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()," ");
            while (itr.hasMoreTokens()) {
                item.set(itr.nextToken().replace(" ", ""));
                context.write(item, one);
            }
        }
    }

    public static class KTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text item = new Text();
        
        private static void makeCombination(ArrayList<String> str_list, int len, int ind, int k, ArrayList<String> temp,
                ArrayList<String> kpairs) {
            if (k == 0) {
                kpairs.add(temp.toString());
                return;
            }

            for (int i = ind; i < len; i++) {
                temp.add(str_list.get(i));
                makeCombination(str_list, len, i + 1, k - 1, temp, kpairs);
                temp.remove(temp.size() - 1);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()," ");
            ArrayList<String> str_list = new ArrayList<String>();
            while (itr.hasMoreTokens()) {
                str_list.add(itr.nextToken().replace(" ", ""));
            }
            
            int str_list_size = str_list.size();
            Integer pair_size = context.getConfiguration().getInt("iteration", 5);
            ArrayList<String> kpairs = new ArrayList<String>();
            ArrayList<String> temp = new ArrayList<String>();
            makeCombination(str_list, str_list_size, 0, pair_size, temp, kpairs);
            for (String s : kpairs) {
                String st = s;
                item.set(st);
                context.write(item, one);
                
            }
            kpairs.clear();
            temp.clear();
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Integer minimumSupport = context.getConfiguration().getInt("minimumSupport", 10);
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            if (sum > minimumSupport) {
                context.write(key, result);
            }
            return;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Products(), args);
        System.exit(res);
      }



      public int run(String[] args) throws Exception {
    	  int minimumSupport = Integer.parseInt(args[2]);
          int iteration1 = Integer.parseInt(args[3]);
          int iteration2 = Integer.parseInt(args[4]);
          int iteration3 = Integer.parseInt(args[5]);
          int iteration4 = Integer.parseInt(args[6]);
          int iteration5 = Integer.parseInt(args[7]);
          
          //job1
          Configuration conf1 = new Configuration();
          conf1.setInt("minimumSupport", minimumSupport);
          conf1.setInt("iteration", iteration1);
          Job job1 = Job.getInstance(conf1, "Products" );
          job1.setJarByClass(Products.class);
          job1.setMapperClass(TokenizerMapper.class);
          job1.setCombinerClass(IntSumReducer.class);
          job1.setReducerClass(IntSumReducer.class);
          job1.setOutputKeyClass(Text.class);
          job1.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job1, new Path(args[0]));
          FileOutputFormat.setOutputPath(job1, new Path(args[1] + 1));
          job1.waitForCompletion(true);
          
          //job2
          Configuration conf2 = new Configuration();
          conf2.setInt("minimumSupport", minimumSupport);
          conf2.setInt("iteration", iteration2);
          Job job2 = Job.getInstance(conf2, "Products" );
          job2.setJarByClass(Products.class);
          job2.setMapperClass(KTokenizerMapper.class);
          job2.setCombinerClass(IntSumReducer.class);
          job2.setReducerClass(IntSumReducer.class);
          job2.setOutputKeyClass(Text.class);
          job2.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job2, new Path(args[0]));
          FileOutputFormat.setOutputPath(job2, new Path(args[1] + 2));
          job2.waitForCompletion(true);
          
          //job3
          Configuration conf3 = new Configuration();
          conf3.setInt("minimumSupport", minimumSupport);
          conf3.setInt("iteration", iteration3);
          Job job3 = Job.getInstance(conf3, "Products" );
          job3.setJarByClass(Products.class);
          job3.setMapperClass(KTokenizerMapper.class);
          job3.setCombinerClass(IntSumReducer.class);
          job3.setReducerClass(IntSumReducer.class);
          job3.setOutputKeyClass(Text.class);
          job3.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job3, new Path(args[0]));
          FileOutputFormat.setOutputPath(job3, new Path(args[1] + 3));
          job3.waitForCompletion(true);
          
          //job4
          Configuration conf4 = new Configuration();
          conf4.setInt("minimumSupport", minimumSupport);
          conf4.setInt("iteration", iteration4);
          Job job4 = Job.getInstance(conf4, "Products" );
          job4.setJarByClass(Products.class);
          job4.setMapperClass(KTokenizerMapper.class);
          job4.setCombinerClass(IntSumReducer.class);
          job4.setReducerClass(IntSumReducer.class);
          job4.setOutputKeyClass(Text.class);
          job4.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job4, new Path(args[0]));
          FileOutputFormat.setOutputPath(job4, new Path(args[1] + 4));
          job4.waitForCompletion(true);
          
          //job5
          Configuration conf5 = new Configuration();
          conf5.setInt("minimumSupport", minimumSupport);
          conf5.setInt("iteration", iteration5);
          Job job5 = Job.getInstance(conf5, "Products" );
          job5.setJarByClass(Products.class);
          job5.setMapperClass(KTokenizerMapper.class);
          job5.setCombinerClass(IntSumReducer.class);
          job5.setReducerClass(IntSumReducer.class);
          job5.setOutputKeyClass(Text.class);
          job5.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job5, new Path(args[0]));
          FileOutputFormat.setOutputPath(job5, new Path(args[1] + 5));
          return job5.waitForCompletion(true) ? 0 : 1;
      } 
}
