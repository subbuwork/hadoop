package com.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CharCountMpper extends Mapper< LongWritable, Text, Text , IntWritable > {
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		char[] charArray = line.toCharArray();
		for(char c :charArray){
			System.out.println("Character:::"+c);
			context.write(new Text(String.valueOf(c)),new IntWritable(1));
		}
	}

}
