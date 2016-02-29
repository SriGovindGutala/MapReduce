package com.erp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomFileInputFormat extends FileInputFormat<LongWritable,Text>
{
	  public static final long MAX_SPLIT_SIZE = 9388608;
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
    		InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException 
    {
	      return new CustomLineRecordReader();
    }

    protected long computeSplitSize(long blockSize, long minSize, long maxSize)
    {
    	return MAX_SPLIT_SIZE;
    }
}