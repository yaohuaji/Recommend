package com.yhj.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReduce extends Reducer<SortKey, Text, Text, Text>{
	@Override
	protected void reduce(SortKey key, Iterable<Text> values,
			Reducer<SortKey, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		String user = key.getUser();
		for(Text value : values){
			sb.append(value.toString() + ",");
			i++;
			if(i > 9)
				break;
		}
		context.write(new Text(user), new Text(sb.toString()));
	}
}
