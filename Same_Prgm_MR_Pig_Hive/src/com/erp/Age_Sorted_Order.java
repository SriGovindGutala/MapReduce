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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.erp.Highest_Salary.IntSumReducer;
import com.erp.Highest_Salary.TokenizerMapper;

public class Age_Sorted_Order 
{


	  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable,Text>
	  {  
	    private static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	
	      String [] empdata = value.toString().split(",");
	      String worddata = empdata[1] +","+ empdata[4];
	      int age = Integer.parseInt(empdata[2]);
	  	  //HM_emp.put(empdata[1], Integer.parseInt(empdata[4]));
	      
	      // int j = Integer.parseInt(empdata[4]);
	      
          word.set(worddata);
          context.write(one,word);
	    }
	  }

	  public static class IntSumReducer extends Reducer<IntWritable,Text,Text,IntWritable> 
	  {
	    private IntWritable result = new IntWritable();
	    private Text word1 = new Text();
	    private HashMap<String,Integer> HM_emp = new HashMap<String,Integer>();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
	      for (Text val : values) 
	      {
	    	  String [] empdata1 = val.toString().split(",");
		      HM_emp.put(empdata1[0], Integer.parseInt(empdata1[1]));
	      }
	      int maxValueInMap=(Collections.max(HM_emp.values()));  // This will return max value in the Hash map
	        for (Entry<String, Integer> entry : HM_emp.entrySet()) {  // Iterate through hash map
	            if (entry.getValue()==maxValueInMap) 
	            {
	            	word1.set(entry.getKey().toString()); 	                 // Print the key with max value
	            }
	        }
	      result.set(maxValueInMap);
	      context.write(word1, result);
	    }
	  }

	  public static void main(String[] args) throws Exception 
	  {
	    Configuration conf = new Configuration();
	  //  DistributedCache.addCacheFile(new URI("Govind/Inputs/MR_Pig_Hive/empdetails.txt"),conf);
	    Job job = Job.getInstance(conf, "Highest Salary");
	    job.setJarByClass(Highest_Salary.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(1);
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
