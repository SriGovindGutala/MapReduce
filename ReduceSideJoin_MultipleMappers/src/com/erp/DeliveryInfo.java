package com.erp;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeliveryInfo 
{
	public static class UserFileMapper extends Mapper  <LongWritable, Text, Text, Text>
	{
		//variables to process Consumer Details
		private String IDnum,customerName, fileTag= "CD~";

		/* map method that process ConsumerDetails.txt and frames the initial key value pairs
		 * Key(Text) – mobile number
		 * Value(Text) – An identifier to indicate the source of input(using ‘CD’ for the customer details file) + Customer Name
		 * */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			//taking one line/record at a time and parsing them into key value pairs
			String line = value.toString();
			String splitarray[] = line.split(",");
			IDnum = splitarray[0].trim();
			customerName = splitarray[1].trim();

			//sending the key value pair out of mapper
			context.write(new Text(IDnum), new Text(fileTag+customerName));
		}
	}
	public static class DeliverFileMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		//variables to process delivery report
		private String IDnum,deliveryCode,fileTag="DR~";

		/* map method that process DeliveryReport.txt and frames the initial key value pairs
		 *Key(Text) – mobile number
		 *Value(Text) – An identifier to indicate the source of input(using ‘DR’ for the delivery report file) + Status Code*/

		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			//taking one line/record at a time and parsing them into key value pairs
			String line = value.toString();
			String splitarray[] = line.split(",");	
			IDnum = splitarray[0].trim();
			deliveryCode = splitarray[1].trim();

			//sending the key value pair out of mapper
			context.write(new Text(IDnum), new Text(fileTag+deliveryCode));
		}
	}
		public static class TokenizerReducer extends Reducer<Text, Text, Text, Text>
		{

			//Variables to aid the join process
			private String customerName,deliveryReport;
			private BufferedReader brReader;
			enum MYCOUNTER 
			{
				RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
			}
			/*Map to store Delivery Codes and Messages
			*Key being the status code and vale being the status message*/
			private static HashMap<String,String> DeliveryCodesMap= new HashMap<String,String>();
		
			@Override
			protected void setup(Context context) throws IOException,InterruptedException 
			{
				Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				for (Path eachPath : cacheFilesLocal) 
				{
					if (eachPath.getName().toString().trim().equals("DeliveryStatusCodes")) 
					{
						context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
						loadDeliveryStatusCodes(eachPath, context);
					}
				}
			}
		
			//To load the Delivery Codes and Messages into a hash map
			private void loadDeliveryStatusCodes(Path filePath, Context context) throws IOException
			{
				String strLineRead = "";
				try 
				{
				brReader = new BufferedReader(new FileReader(filePath.toString()));
			
					// Read each line, split and load to HashMap
					while ((strLineRead = brReader.readLine()) != null) 
					{
						String splitarray[] = strLineRead.split(",");
						DeliveryCodesMap.put(splitarray[0].trim(),    splitarray[1].trim());
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
		
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
			{
				for (Text value : values)
				{
					String currValue = value.toString();
					String valueSplitted[] = currValue.split("~");
					/*identifying the record source that corresponds to a cell number
					*  and parses the values accordingly*/
					if(valueSplitted[0].equals("CD"))
					{
						customerName=valueSplitted[1].trim();
					}
					else if(valueSplitted[0].equals("DR"))
					{
						//getting the delivery code and using the same to obtain the Message
						deliveryReport = DeliveryCodesMap.get(valueSplitted[1].trim());
					}
				}
				//pump final output to file
				if(customerName!=null && deliveryReport!=null)
				{
					context.write(new Text(customerName), new Text(deliveryReport));
				}
				else if(customerName==null)
					context.write(new Text("customerName"), new Text(deliveryReport));
				else if(deliveryReport==null)
					context.write(new Text(customerName), new Text("deliveryReport"));
			}
	
		}
		

		public static void main(String[] args) throws Exception 
		{
			
			Configuration conf = new Configuration();
			DistributedCache.addCacheFile(new URI("Govind/Inputs/DistributedCache/ReduceSideJoin/DeliveryStatusCodes"),conf);
		    Job job = new Job(conf, "Reduce-side join with text lookup file in DCache");
			job.setJarByClass(DeliveryInfo.class);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserFileMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeliverFileMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
          //TextOutputFormat which emits LongWritable key and Text value by default
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(TokenizerReducer.class);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
