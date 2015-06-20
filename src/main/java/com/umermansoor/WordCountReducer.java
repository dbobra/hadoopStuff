package com.umermansoor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		int sum = 0;
		for(IntWritable a : arg1){
			sum+= a.get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}
	
	

}
