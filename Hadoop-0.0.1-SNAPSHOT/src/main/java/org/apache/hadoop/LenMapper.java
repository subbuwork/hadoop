package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		for(String word:line.split(" ")){
			if(word.length()>0){
				context.write(new Text(word),new IntWritable(word.length()));	
			}	
		}
	}
}
