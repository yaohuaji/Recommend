package com.yhj.tongxian;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TongxianMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String [] tokens = Pattern.compile("[/t,]").split(value.toString());
		String user = tokens[0];
		for(int i = 1; i < tokens.length;i++){
			String item1 = tokens[i].split(":")[0];
			for(int j = i+1; j< tokens.length;j++){
				String item2 = tokens[j].split(":")[0];
				context.write(new Text(item1 + ":" +item2), new Text("1"));
			}
			
		}
	}

}
