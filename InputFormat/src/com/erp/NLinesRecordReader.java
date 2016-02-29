package com.erp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class NLinesRecordReader extends RecordReader<LongWritable, Text> 
{
    private final int NLINESTOPROCESS = 1;
    private LineReader in;
    private LongWritable key = new LongWritable();
    private Text value = new Text();	
    private long start;
    private long pos;
    private long end;
    private int maxLineLength;

    
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }
 
    /**
     * From Design Pattern, O'Reilly...
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
 
    /**
     * From Design Pattern, O'Reilly...
     * Like the corresponding method of the InputFormat class, this is an
     * optional method used by the framework for metrics gathering.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }
 
    /**
     * From Design Pattern, O'Reilly...
     * This method is used by the framework for cleanup after there are no more
     * key/value pairs to process.
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	public boolean nextKeyValue1() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}
    public void initialize1(InputSplit genericSplit, TaskAttemptContext context) throws IOException 
    {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            --start;
            fileIn.seek(start);
        }
 
        in = new LineReader(fileIn, job);
        if (skipFirstLine) {
            Text dummy = new Text();
            start += in.readLine(dummy, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
 
    }
    public boolean nextKeyValue() throws IOException 
    {
    	if(key == null){
    		key = new LongWritable();
    	}
        key.set(pos);
        if (value == null){
        	value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;
        for(int i = 0;i<NLINESTOPROCESS;i++){
           Text v = new Text();
           while (pos < end) {
               newSize = in.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),maxLineLength));
               value.append(v.getBytes(), 0,v.getLength());
               value.append(endline.getBytes(),0,endline.getLength());
               if (newSize == 0) {
                  break;
               }
               pos += newSize;
               if (newSize < maxLineLength) {
                   break;
               }
            LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
             return true;
        }
    }
}
