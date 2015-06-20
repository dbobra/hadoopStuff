package com.umermansoor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdPartyReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String[] str = value.toString().split(";",12);
		
		// Ignore invalid lines
        if (str.length != 12) {
            System.out.println("- " + str.length);
            return;
        }
		
		String outputKey = str[2];
		
		word.set(outputKey);
		context.write(word, one);
		
	}
	
	

}
