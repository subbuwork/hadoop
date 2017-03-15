package com.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	Integer count = 0;
	IntWritable result = new IntWritable();
	for(IntWritable value: values){
		count+=value.get();
		result.set(count);
			
	}
	String found = key.toString();
	if(found.equals("a")|| found.equals("b")|| found.equals("c")){
		System.out.println("Character:::"+found);
		arg2.write(key,result);
	}
	
	
	}

}
