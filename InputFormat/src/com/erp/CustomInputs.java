package com.erp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.erp.CustomInputs.IntSumReducer;
import com.erp.CustomInputs.TokenizerMapper;

public class CustomInputs {
	
	  public static class TokenizerMapper extends Mapper<Object, Text, LongWritable, Text>
	  {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	     // word.set
	      context.write((LongWritable) key, value);
	    }
	  }

	  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	  {
	    private IntWritable result = new IntWritable();
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	    {
	      int sum = 0;
	      for (IntWritable val : values) 
	      {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }
	  
	  public static void main(String[] args) throws Exception 
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Custom Inputs");
	    job.setJarByClass(CustomInputs.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(0);
	   // job.setCombinerClass(IntSumReducer.class);
	   // job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(CustomFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
