package com.dv;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctValueDriver {

	  public static void main(String[] args) throws Exception {
		  
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: DistinctValue <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    Job job = new Job();
	    job.setJarByClass(DistinctValueDriver.class);
	    job.setJobName("DistinctValue");
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setMapperClass(DistinctValueMapper.class);
	    job.setReducerClass(DistinctValueReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	  }
	}
