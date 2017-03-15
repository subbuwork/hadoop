package org.apache.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Driver.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(RedForWord.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		System.out.println("Job Completed!!!!!");

	}

}
