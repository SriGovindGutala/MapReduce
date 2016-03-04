package com.erp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.erp.DCExample1.IntSumReducer;
import com.erp.DCExample1.TokenizerMapper;

public class DCExample1 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	  {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private BufferedReader brReader;
	 //   ArrayList<String> Offset = new ArrayList<String>();
	    
	     private String cache [] = new String[10];
	     protected void setup(Context context) throws IOException, InterruptedException 
	     {
	    	Path[] cacheFiles = context.getLocalCacheFiles();
	    	//FileInputStream fileStream = new FileInputStream(cacheFiles[0].toString());
	    	brReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
	    	//.getName().
	    	String strLineRead;
	    	int i = 0;
			while ((strLineRead = brReader.readLine()) != null) 
			{
	    		//Offset.add(strLineRead);
				cache[i] = strLineRead;
				i++;
			}
	      }
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	// StringTokenizer itr = new StringTokenizer(value.toString(),",");
	    	
	    	String s [] = value.toString().split(",");
	    	
	    	
	    	if(s[1].equals(cache[0]) || s[1].equals(cache[1]))
	    	{
	    	word.set(s[0]);
             context.write(word, one); 
	    	}
           
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
	    DistributedCache.addCacheFile(new URI("Govind/Inputs/DistributedCache/offset"),conf);
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(DCExample1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(0);
	   // job.setCombinerClass(IntSumReducer.class);
	  //  job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}
