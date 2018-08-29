package com.yhj.duplicate.removal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RemoveRunner {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node8:8020");
		config.set("yarn.resourcemanager.hostname","node8");
		
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("duplicate rempval");
			job.setJarByClass(RemoveRunner.class);
			
			job.setMapperClass(RemoveMapper.class);
			job.setReducerClass(RemoveReduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			FileInputFormat.addInputPath(job, new Path("/root/input/tuijian"));
			
			Path output = new Path("/recommend/output/removalresult");
			if(fs.exists(output)){
				fs.delete(output);
			}
			FileOutputFormat.setOutputPath(job, output);
			boolean result = job.waitForCompletion(true);
			if(result){
				System.out.println("removal succeed");
			}
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
