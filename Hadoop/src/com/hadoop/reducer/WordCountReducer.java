package com.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
@Override
protected void reduce(Text key, Iterable<IntWritable> values,Context context)
		throws IOException, InterruptedException {
	Integer count = 0;
	IntWritable result =  new IntWritable();
	for(IntWritable value : values){
		count += value.get();
		result.set(count);
	}
	context.write(key,result);
}
}
