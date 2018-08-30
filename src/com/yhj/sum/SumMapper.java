package com.yhj.sum;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String tokens [] = Pattern.compile("[\t,]").split(value.toString());
		String user = tokens[0];
		String item = tokens[1];
		String score = tokens[2];
		context.write(new Text(user), new Text(item + ":" +score));
	}
}
