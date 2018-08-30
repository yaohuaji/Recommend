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

import com.yhj.like.multi.tongxian.MultiMapper;
import com.yhj.like.multi.tongxian.MultiReduce;
import com.yhj.tongxian.TongxianMapper;
import com.yhj.tongxian.TongxianReduce;
import com.yhj.userlike.UserLikeMapper;
import com.yhj.userlike.UserLikeReduce;

public class RemoveRunner {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node8:8020");
		config.set("yarn.resourcemanager.hostname","node8");
		
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("tongxian job");
			job.setJarByClass(RemoveRunner.class);
			
			job.setMapperClass(MultiMapper.class);
			job.setReducerClass(MultiReduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, 
					new Path []{new Path("/recommend/output/userlikeresult"),
					new Path("/recommend/output/tongxian")}
					);
//			FileInputFormat.addInputPath(job, new Path("/recommend/output/userlikeresult"));
			
			Path output = new Path("/recommend/output/mutil");
			if(fs.exists(output)){
				fs.delete(output);
			}
			FileOutputFormat.setOutputPath(job, output);
			boolean result = job.waitForCompletion(true);
			if(result){
				System.out.println("userlike succeed");
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
