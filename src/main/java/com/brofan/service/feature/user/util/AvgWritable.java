package com.brofan.service.feature.user.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class AvgWritable implements Writable{
	private IntWritable num;
	private FloatWritable sum;
	
	public AvgWritable() {
		this.num = new IntWritable();
		this.sum = new FloatWritable();
	}
	
	public AvgWritable(int num, Float sum) {
		this.num = new IntWritable (num);
		this.sum = new FloatWritable (sum);
	}

	public void readFields(DataInput in) throws IOException {
		num.readFields(in);
		sum.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		num.write(out);
		sum.write(out);
	}
	
	public IntWritable getNum() {
		return num;
	}

	public FloatWritable getSum() {
		return sum;
	}
}
