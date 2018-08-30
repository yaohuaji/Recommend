package com.yhj.userlike;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserLikeReduce extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		Map<String,Integer> map = new HashMap<String,Integer>();
		StringBuffer sb = new StringBuffer();
		for(Text i : value){
			String tokens [] = i.toString().split(":");
			String item = tokens[0];
			int score = Integer.parseInt(tokens[1]);
			if(map.containsKey(item)){
				map.put(item, map.get(item)+score);
			}else{
				map.put(item, score);
			}
		}
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			sb.append(entry.getKey() + ":" + entry.getValue() + ",");
		}
		context.write(key, new Text(sb.toString()));
	}

}
