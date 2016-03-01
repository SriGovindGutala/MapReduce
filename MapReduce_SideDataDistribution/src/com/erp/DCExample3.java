package com.erp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.erp.DCExample3.IntSumReducer;
import com.erp.DCExample3.TokenizerMapper;

public class DCExample3 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	  {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private BufferedReader brReader;
	    private String Delimit,Record_Number;
        
	    private HashMap<String,Integer> HM = new HashMap<String,Integer>();
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	 
	        // Assigning the Variables from Configuration file //
	    	Delimit = context.getConfiguration().get("Delimiter");
	    	Record_Number = context.getConfiguration().get("Number_Of_Records");
	    	
	        // Reading from a Distributed Cache file //	    	
	    	Path[] cacheFiles = context.getLocalCacheFiles();
	    	for(int i=0;i<cacheFiles.length;i++)
	    	 {
	    		brReader = new BufferedReader(new FileReader(cacheFiles[i].toString()));
	    	 }
	    	
		    String Cache_Data_Line = brReader.readLine();
   	        while((Cache_Data_Line = brReader.readLine()) != null)
		    {
                String Cache_Data [] = Cache_Data_Line.split(",");
	    		HM.put(Cache_Data[0], Integer.parseInt(Cache_Data[1]));
            }
		    
	    }
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	// StringTokenizer itr = new StringTokenizer(value.toString(),",");
	        
	    	String s [] = value.toString().split(Delimit);
	    	
	    	if(HM.containsKey(s[0]))
	    	{
	    		if(Integer.parseInt(s[1]) == HM.get(s[0]))
	    		{
	    			word.set(s[0]);
	    		}
	    	}
	    	context.write(word, one);
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
	    DistributedCache.addCacheFile(new URI("Govind/Inputs/DistributedCache/SideData/cache.txt"),conf);

	    //  sending the offset values from the configuration file   //
	    FileSystem FS = FileSystem.get(conf);
	    Path hdfsfilepath = new Path("Govind/Inputs/DistributedCache/SideData/metadata.txt");
	   // BufferedReader bufreaderr = new BufferedReader(new FileReader(hdfsfilepath.toString()));
	    BufferedReader bufreader = new BufferedReader(new InputStreamReader(FS.open(hdfsfilepath)));
	    String offset = bufreader.readLine();
	    String offset_values [] = offset.split("|");
        conf.set("Delimiter", offset_values[0]);
        conf.set("Number_Of_Records", offset_values[1]);
        
	    //  setting all other job config's   //
        Job job = new Job(conf, "Side Data Distribution");
	    job.setJarByClass(DCExample3.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(0);
	 // job.setCombinerClass(IntSumReducer.class);
	 // job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}
