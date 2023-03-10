package com.mmtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MinMaxTempDriver{
	public static void main(String[] args) throws Exception {
		
	Configuration conf = new Configuration();
	Job job = new Job(conf, "weather example");
	job.setJarByClass(MinMaxTempDriver.class); 
	
	job.setMapOutputKeyClass(Text.class); 
	job.setMapOutputValueClass(Text.class);
	
	job.setMapperClass(MinMaxTempMapper.class); 	
	job.setReducerClass(MinMaxTempReducer.class);
	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	
	
	Path OutputPath = new Path(args[1]);
	
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	
	OutputPath.getFileSystem(conf).delete(OutputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}