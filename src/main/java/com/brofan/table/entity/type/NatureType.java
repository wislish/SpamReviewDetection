package com.brofan.table.entity.type;

import org.apache.hadoop.hbase.util.Bytes;

public class NatureType {
	private String str;
	private byte[] colName;
	
	public NatureType(String str) {
		this.str = str;
		colName = Bytes.toBytes(str);
	}
	
	public String getStr() {
		return str;
	}

	public byte[] getColName() {
		return colName;
	}
}
