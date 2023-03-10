package com.mmtemp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class MinMaxTempMapper extends Mapper <LongWritable, Text, Text, Text> {

@Override
	public void map(LongWritable arg0, Text Value, Context context)
			throws IOException, InterruptedException {


		String line = Value.toString();


		if (!(line.length() == 0))
		{

			String date = line.substring(6, 14); 

			float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
			float temp_Min = Float.parseFloat(line.substring(47, 53).trim()); 


			if (temp_Max > 35.0) 
				context.write(new Text("Hot Day " + date),new Text(String.valueOf(temp_Max))); 


			if (temp_Min < 10) 
				context.write(new Text("Cold Day " + date),new Text(String.valueOf(temp_Min))); 
		
		}
	}

}