package com.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	  /*
	   * The reduce method runs once for each key received from
	   * the shuffle and sort phase of the MapReduce framework.
	   * The method receives a key of type Text, a set of values of type
	   * IntWritable, and a Context object.
	   */
	  @Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int wordCount = 0;
			
			/*
			 * For each value in the set of values passed to us by the mapper:
			 */
			for (IntWritable value : values) {
			  
			  /*
			   * Add the value to the word count counter for this key.
			   */
				wordCount += value.get();
			}
			
			/*
			 * Call the write method on the Context object to emit a key
			 * and a value from the reduce method. 
			 */
			context.write(key, new IntWritable(wordCount));
		}
	}
