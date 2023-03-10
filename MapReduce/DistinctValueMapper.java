package com.dv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	for (String data : line.split("\\W+")) {
	      if (data.length() > 0) {
	         
	    	  context.write(new Text(data), new IntWritable(1));
	      }
	    }
	}
	}