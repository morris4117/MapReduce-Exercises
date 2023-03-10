package com.oe;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OddEvenDriver extends Configured implements Tool {

@Override
public int run(String[] args) throws Exception
{
if (args.length < 2)
{
System.out.println("Please enter valid arguments");
return -1;
}

JobConf conf = new JobConf(OddEvenDriver.class);
FileInputFormat.setInputPaths(conf, new Path(args[0]));
FileOutputFormat.setOutputPath(conf, new Path(args[1]));
conf.setMapperClass(OddEvenMapper.class);
conf.setReducerClass(OddEvenReducer.class);
conf.setMapOutputKeyClass(Text.class);
conf.setMapOutputValueClass(IntWritable.class);
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(IntWritable.class);

JobClient.runJob(conf);
return 0;
}

// Main Method
public static void main(String args[]) throws Exception
{
int exitcode = ToolRunner.run(new OddEvenDriver(), args);
System.out.println(exitcode);
}
}
