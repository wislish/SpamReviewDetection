package com.brofan.service.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyBuilder {
	
	private List<String> array;
	private Timestamp ts;
	
	public RowKeyBuilder(Timestamp ts) {
		this.ts = ts;
		array = new ArrayList<String>();
	}
	
	public void append(String s) {
		array.add(s);
	}
	
	public byte[] build() {
		StringBuffer sb = new StringBuffer();
		for (String s : array) {
			sb.append(s);
			sb.append("_");
		}
		
		// 按时间顺序
		sb.append(ts.getTime());
		
		return Bytes.toBytes( sb.toString() );
	}
}
