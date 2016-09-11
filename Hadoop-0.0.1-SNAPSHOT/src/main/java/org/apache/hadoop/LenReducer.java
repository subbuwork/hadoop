package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	String maxWord;
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{
		maxWord = new String();	
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> vList,Context context)
			throws IOException, InterruptedException {
		
		if(key.toString().length()>maxWord.length()){
			maxWord = key.toString();
		}
	}
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		context.write(new Text(maxWord), new IntWritable(maxWord.length()));
	}

}
