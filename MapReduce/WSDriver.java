package com.ws;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WSDriver {
	public static void main(String[] args) throws Exception {
        try {
            if (args.length<3) {
                System.err.println("Give the input/ output/ keyword!");
                return;
            }
            Configuration conf = new Configuration();
            conf.set("search",args[4]);
            Job job = new Job(conf);
            job.setJobName("WordSearch");
            

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileInputFormat.addInputPath(job, new Path(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]));
            

            job.setJarByClass(WSDriver.class);
            
            job.setMapperClass(WSMapper.class);
            job.setReducerClass(WSReducer.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

             
            boolean success = job.waitForCompletion(true);
            System.exit(success?0:1);
       }    
       catch (Exception e) {
           e.getMessage();
       }
  }    
}