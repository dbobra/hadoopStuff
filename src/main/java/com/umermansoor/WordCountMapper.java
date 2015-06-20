package com.umermansoor;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	//hadoop supported data types
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString(); 
		StringTokenizer stringTokenizer = new StringTokenizer(line);
		while(stringTokenizer.hasMoreTokens()){
			word.set(stringTokenizer.nextToken());
		    context.write(word,one);
		}
	}

	
		
	}
	
	


