package com.yhj.userlike;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserLikeReduce extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		StringBuffer sb = new StringBuffer();
		for(Text i : value){
			sb.append(i.toString() + ",");
		}
		
		context.write(key, new Text(sb.toString()));
	}

}
