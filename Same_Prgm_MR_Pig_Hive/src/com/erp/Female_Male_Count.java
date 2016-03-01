package com.erp;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Female_Male_Count {
	  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable,Text>
	  {  
	    private IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	
	      String [] empdata = value.toString().split(",");
	      String worddata = empdata[3];
	      if (worddata.equals("f"))
	      {
	    	  this.one = new IntWritable(2);  
	      }else{
	    	  this.one = new IntWritable(1);
	      }
	    	  word.set(worddata);
	          context.write(one,word);
	    }
	  }

	  public static class IntSumReducer extends Reducer<IntWritable,Text,Text,IntWritable> 
	  {
	    private IntWritable result = new IntWritable();
	    private Text word1 = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
		      int sum = 0;
		      String gender = "";
		      for (Text val : values) 
		      {
		       sum++;
		       gender = val.toString();
		      }
		      word1.set(gender);
		      result.set(sum);
		      context.write(word1, result);
	    }
	  }

	  public static void main(String[] args) throws Exception 
	  {
	    Configuration conf = new Configuration();
	  //  DistributedCache.addCacheFile(new URI("Govind/Inputs/MR_Pig_Hive/empdetails.txt"),conf);
	    Job job = Job.getInstance(conf, "Highest Salary");
	    job.setJarByClass(Female_Male_Count.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setNumReduceTasks(1);
	   // job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	

}
