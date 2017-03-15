package org.apache.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable,Text, Text,IntWritable>{
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer t = new StringTokenizer(line);
		while(t.hasMoreTokens()){
			String word = t.nextToken();
			context.write(new Text(word),new IntWritable(1));
		}
	
	}

}
