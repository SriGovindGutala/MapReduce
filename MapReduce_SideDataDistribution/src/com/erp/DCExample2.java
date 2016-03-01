package com.erp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
//import java.nio.file.FileSystem;
import java.util.ArrayList;
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

import com.erp.DCExample2.TokenizerMapper;


public class DCExample2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	  {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private BufferedReader brReader;
	    private String secure = null;
	    // ArrayList<String> Offset = new ArrayList<String>();
	    
	     private String cache [] = new String[10];
	     
	     protected void setup(Context context) throws IOException, InterruptedException 
	     {
                secure = context.getConfiguration().get("HELLO_CONFIG");
	     }
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	    {
	    	// StringTokenizer itr = new StringTokenizer(value.toString(),",");
            String[] S = value.toString().split(",");
            S[S.length+1]= secure;
            String textfinal = new String();
            for(String textfin: S)
            {
            	textfin = textfin + S;
            	textfinal = textfin;
            }
	    	word.set(textfinal);
            context.write(word, one); 
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
        FileSystem fs = FileSystem.get(conf); 
        Path hdfsfilePath = new Path("Govind/Inputs/DistributedCache/offset");
        BufferedReader BufReader = new BufferedReader(new FileReader(hdfsfilePath.toString()));
        String S = BufReader.readLine();
        conf.set("HELLO_CONFIG", S);
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(DCExample2.class);
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
}
