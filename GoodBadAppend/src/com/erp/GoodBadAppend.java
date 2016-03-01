package com.erp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.erp.GoodBadAppend.IntSumReducer;
import com.erp.GoodBadAppend.TokenizerMapper;

public class GoodBadAppend 
{


	public static class TokenizerMapper extends Mapper<Object, Text,Text,NullWritable>
	  {
	    //private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	      boolean res,good,bad;
	      
	      bad = false;
	      good = false;
	     
	      ArrayList<String> data = new ArrayList<String>();
	      StringTokenizer itr = new StringTokenizer(value.toString(),",");
	      while (itr.hasMoreTokens()) 
	      {
	    	data.add(itr.nextToken());
	      }
	   // if there is no data in the array, it's a bad array
	     if(data.size() < 4)
	     {
	    	  bad = true;
	     }
	     else
	     {
	      for(int i=0;i<data.size();i++)
	      {
	       if(i==1)
	       {
	   	      // if the second column is not integer, it's a bad array
	 	      res = checkstring(data.get(i));
	 	      if(res != true)
	 	      {
	 	    	  bad = true;
	 	      }   
	       }
	       else
	       {
	         if(data.get(i).isEmpty())
	         {
	        	bad = true;
	         }
	       }
	      }
	     }
	      if (bad == true)
	      {	        	
		        word.set(value.toString()+"bad");
	        	context.write(word,NullWritable.get());
	      }
	      else
	      {
		        word.set(value.toString()+"good");
		        context.write(word,NullWritable.get()); 
	      }
	    }
	  }

	public static boolean checkstring(String s) 
	{	
	   boolean isInt = false;
	   try 
	   { 
	      Integer.parseInt(s);
	      isInt = true;
	   } 
	   catch (NumberFormatException e) 
	   {
	     isInt = false;
	   }
	  return isInt;
    }
	  public static class IntSumReducer extends Reducer<Text,NullWritable,Text,NullWritable> 
	  {
	   // private IntWritable result = new IntWritable();
	    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException 
	    {
	      
	      context.write(key, NullWritable.get());
	    }
	  }

	  public static void main(String[] args) throws Exception 
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(GoodBadAppend.class);
	    job.setMapperClass(TokenizerMapper.class);
	   // job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	

	
}
