package com.yhj.like.multi.tongxian;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MultiMapper extends Mapper<LongWritable, Text, Text, Text>{
	String flag = "";
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		flag = fs.getPath().getParent().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String tokens [] = Pattern.compile("[\t,]").split(value.toString());
		
		if(flag.equals("step3")){
			String item1 = tokens[0].split(":")[0];
			String item2 = tokens[0].split(":")[1];
			String num = tokens[1];
			context.write(new Text(item1), new Text("A:" + item2 + "\t" + num));
		}else if(flag.equals("userlikeresut")){
			String user = tokens[0];
			for(int i = 1;i < tokens.length;i++){
				String item = tokens[i].split(":")[0];
				String num = tokens[i].split(":")[1];
				context.write(new Text(item), new Text("B:" + user + "\t" + num));
			}
		}
	}

}
