package com.erp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin 
{

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> 
	{

		private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
		private BufferedReader brReader;
		private String strDeptName = "";
		private Text txtMapOutputKey = new Text("");
		private Text txtMapOutputValue = new Text("");

		enum MYCOUNTER 
		{
			RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{

			Path[] AllcacheFiles = context.getLocalCacheFiles();
			//loadDepartmentsHashMap(AllcacheFiles[0], context);
			
			for (Path eachFilepath : AllcacheFiles) 
			{
				if (eachFilepath.getName().toString().trim().equals("departments")) 
				{
					context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
					loadDepartmentsHashMap(eachFilepath, context);
				}
			}
			
		}

		private void loadDepartmentsHashMap(Path filePath, Context context) throws IOException 
		{
			String strLineRead = "";
			try 
			{
				brReader = new BufferedReader(new FileReader(filePath.toString()));
				while ((strLineRead = brReader.readLine()) != null) 
				{
					String deptFieldArray[] = strLineRead.split("\t");
					DepartmentMap.put(deptFieldArray[0].trim(),deptFieldArray[1].trim());
				}
			} 
			catch (FileNotFoundException e) 
			{
				e.printStackTrace();
				context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
			} 
			catch (IOException e) 
			{
				context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
				e.printStackTrace();
			}
			finally 
			{
				if (brReader != null) 
				{
					brReader.close();
				}

			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
			if (value.toString().length() > 0) 
			{
				String Employee_data[] = value.toString().split("\\t");
				
				try 
				{
					strDeptName = DepartmentMap.get(Employee_data[6].toString());
				} finally 
				{
					strDeptName = ((strDeptName.equals(null) || strDeptName.equals("")) ? "NOT-FOUND" : strDeptName);
				}

				txtMapOutputKey.set(Employee_data[0].toString());

				txtMapOutputValue.set(Employee_data[1].toString() + "\t"
						            + Employee_data[2].toString() + "\t"
						            + Employee_data[3].toString() + "\t"
						            + Employee_data[4].toString() + "\t"
						            + Employee_data[5].toString() + "\t"
						            + Employee_data[6].toString() + "\t" 
						            + strDeptName);
			}
			context.write(txtMapOutputKey, txtMapOutputValue);
			strDeptName = "";
		}
	}
	public static void main(String[] args) throws Exception 
	{
			Configuration conf = new Configuration();
			DistributedCache.addCacheFile(new URI("Govind/Inputs/DistributedCache/departments"),conf);
		    Job job = new Job(conf, "Map-side join");
			job.setJarByClass(TokenizerMapper.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(TokenizerMapper.class);
			job.setNumReduceTasks(0);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
