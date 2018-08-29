package com.yhj.tongxian;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TongxianReduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		int sum = 0;
		for(Text i : value){
			sum += Integer.parseInt(i.toString());
		}
		context.write(key, new Text(String.valueOf(sum)));
	}
}
