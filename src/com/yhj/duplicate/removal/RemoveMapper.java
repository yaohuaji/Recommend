package com.yhj.duplicate.removal;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RemoveMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		if(key.get() != 0){
			context.write(value, NullWritable.get());
		}
	}
}
