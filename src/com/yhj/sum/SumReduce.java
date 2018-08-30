package com.yhj.sum;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReduce extends Reducer<Text, Text, Text, Text>{
	 @Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		 Map<String ,Integer> map = new HashMap<String,Integer>();
		 
		 for(Text t : value){
			 String tokens [] = t.toString().split(":");
			 if(map.containsKey(tokens[0])){
				 map.put(tokens[0], map.get(tokens[0]).intValue() + Integer.parseInt(tokens[1]));
			 }else{
				 map.put(tokens[0], Integer.parseInt(tokens[1]));
			 }
		 }
		 
		 for(Map.Entry<String, Integer> entry : map.entrySet()){
			 context.write(new Text(key), new Text(entry.getKey() + ":" + entry.getValue()));
		 }
	}

}
