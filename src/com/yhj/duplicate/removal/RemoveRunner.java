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
import com.yhj.sort.SortClass;
import com.yhj.sort.SortGroup;
import com.yhj.sort.SortKey;
import com.yhj.sort.SortMapper;
import com.yhj.sort.SortReduce;
import com.yhj.sum.SumMapper;
import com.yhj.sum.SumReduce;
import com.yhj.tongxian.TongxianMapper;
import com.yhj.tongxian.TongxianReduce;
import com.yhj.userlike.UserLikeMapper;
import com.yhj.userlike.UserLikeReduce;

public class RemoveRunner {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node1:8020");
		config.set("yarn.resourcemanager.hostname","node1");
		
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("sort job");
			job.setJarByClass(RemoveRunner.class);
			
			job.setMapperClass(SortMapper.class);
			job.setReducerClass(SortReduce.class);
			
			job.setMapOutputKeyClass(SortKey.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setSortComparatorClass(SortClass.class);
			job.setGroupingComparatorClass(SortGroup.class);
			
			/*FileInputFormat.setInputPaths(job, 
					new Path []{new Path("/recommend/output/userlikeresult"),
					new Path("/recommend/output/tongxian")}
					);*/
			
			FileInputFormat.addInputPath(job, new Path("/recommend/output/sum"));
			
			Path output = new Path("/recommend/output/finaltuijian");
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
