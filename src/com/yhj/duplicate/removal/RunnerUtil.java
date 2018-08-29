package com.yhj.duplicate.removal;

import java.util.HashMap;
import java.util.Map;

public class RunnerUtil {
	public static Map<String,Integer> SCORE = new HashMap<String,Integer>();
	static {
		SCORE.put("click", 1);
		SCORE.put("collect", 2);
		SCORE.put("cart", 3);
		SCORE.put("alipay", 4);
	}
}
