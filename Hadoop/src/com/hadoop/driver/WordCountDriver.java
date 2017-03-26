/**
 * 
 */
package com.hadoop.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.mapper.WordCountMapper;
import com.hadoop.reducer.WordCountReducer;

/**
 * @author spark
 *
 */
public class WordCountDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		try {
			Job job = new Job(configuration, "WordCountDriver");
			job.setJarByClass(WordCountDriver.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			System.exit(job.waitForCompletion(true)?0:1);
			 
		}catch (IOException e) {
		  e.printStackTrace();
		}catch (ClassNotFoundException | InterruptedException e) {
			
			e.printStackTrace();
		}

	}

}
