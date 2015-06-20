package com.umermansoor;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdPartyRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		double sum = 0.0;
		int count =0;
		double avg = 0.0;
		
		for (DoubleWritable v : values) {
			System.out.println("Value = "+v.get());
			sum+=v.get();
			count++;
		}
		System.out.println("Sum of total rating "+sum);
		avg = sum/count;
		context.write(arg0, new DoubleWritable(avg));
	}

	
}
