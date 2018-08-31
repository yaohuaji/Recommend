package com.yhj.sort;

import org.apache.hadoop.io.WritableComparator;

public class SortGroup extends WritableComparator{
	public SortGroup() {
		super(SortKey.class,true);
	}
	
	@Override
	public int compare(Object a, Object b) {
		SortKey a1 = (SortKey)a;
		SortKey b1 = (SortKey)b;
		int r = a1.getUser().compareTo(b1.getUser());
		return r;
	}
}
