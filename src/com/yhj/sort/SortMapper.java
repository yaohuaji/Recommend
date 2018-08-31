package com.yhj.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, SortKey, Text>{
	private SortKey sk = new SortKey();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, SortKey, Text>.Context context)
			throws IOException, InterruptedException {
		String tokens [] = value.toString().split("\t");
		String user = tokens[0].toString();
		int score = Integer.parseInt(tokens[1].toString().split(":")[1]);
		sk.setUser(user);
		sk.setScore(score);
		context.write(sk, new Text(tokens[1]));
	}
}
