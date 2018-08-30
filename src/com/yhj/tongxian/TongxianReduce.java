package com.yhj.tongxian;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TongxianReduce extends Reducer<Text, Text, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException,
			InterruptedException {
		int sum = 0;
		for(Text i : value){
			sum += Integer.parseInt(i.toString());
		}
		context.write(key, new IntWritable(sum));
	}
}
