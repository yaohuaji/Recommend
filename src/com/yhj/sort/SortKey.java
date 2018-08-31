package com.yhj.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SortKey implements WritableComparable<SortKey>{
	private String user;
	private int score;
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public int getScore() {
		return score;
	}
	public void setScore(int score) {
		this.score = score;
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		this.user = input.readUTF();
		this.score = input.readInt();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(user);
		output.writeInt(score);
	}
	@Override
	public int compareTo(SortKey o) {
		int r = this.user.compareTo(o.getUser());
		if(r == 0){
			return Integer.compare(this.score, o.getScore());
		}else{
			return r;
		}
	}
}
