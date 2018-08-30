package com.yhj.like.multi.tongxian;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiReduce extends Reducer<Text, Text, Text, Text>{
	
	private Map<String,Integer> tongxian = new HashMap<String,Integer>();
	private Map<String,Integer> userlike = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		for(Text i : value){
			String [] tokens = Pattern.compile("\t").split(i.toString());
			if(tokens[0].startsWith("A")){
				tongxian.put(tokens[0].split(":")[1], Integer.parseInt(tokens[1]));
			}else if(tokens[0].startsWith("B")){
				userlike.put(tokens[0].split(":")[1], Integer.parseInt(tokens[1]));
			}
		}
		
		for(Map.Entry<String, Integer> entry1 : tongxian.entrySet()){
			String item = entry1.getKey();
			int num = entry1.getValue();
			for (Map.Entry<String, Integer> entry2 : userlike.entrySet()){
				String user = entry2.getKey();
				int score = entry2.getValue();
				int result = num*score;
				context.write(new Text(user), new Text(key + ":" + result));
			}
		}
		
	}
}
