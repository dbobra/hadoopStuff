package com.umermansoor;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdPartyRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        String[] str = value.toString().split(";",12);
		/*double outputValue =0.0;*/
		// Ignore invalid lines
        if (str.length != 12) {
            System.out.println("- " + str.length);
            return;
        }
		
		
		try{
			String outputKey = str[2];
		    double outputValue = Double.valueOf(str[4]).doubleValue();
		    context.write(new Text(outputKey), new DoubleWritable(outputValue));
		}catch(Exception e){
			e.printStackTrace();
		}
		
	/*	if(this.isNumeric(str[4])){
			outputValue = Double.parseDouble(str[4]);
		    System.out.println("$$$$$$$$$$$$$");
		}
		else{
			outputValue=Double.parseDouble("0.0");
			System.out.println("##########");
		}*/
		
	
	}

	/*public boolean isNumeric(String str)  
	{  
	  try  
	  {  
	    double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    return false;  
	  }  
	  return true;  
	}*/
	
}
