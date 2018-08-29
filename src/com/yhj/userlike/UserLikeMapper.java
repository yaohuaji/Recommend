package com.yhj.userlike;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yhj.duplicate.removal.RunnerUtil;

public class UserLikeMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String str [] = value.toString().split(",");
		if(str[2].equals("action"))
			return;
		String item = str[0];
		String user = str[1];
		int num = RunnerUtil.SCORE.get(str[2]);
		context.write(new Text(user), new Text(item + ":" + num));
	}

}
