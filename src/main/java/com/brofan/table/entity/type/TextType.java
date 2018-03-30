package com.brofan.table.entity.type;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TextType {
	private String textName;
	private byte[] dataCol = null;
	
	public TextType(String name) {
		this.textName = name;
		getColName();
	}
	
	protected void getColName() {
		if (dataCol == null) {
			dataCol = Bytes.toBytes(textName);
		}
	}
	
	public void putData(Put put, byte[] family, String data) {
		put.add(family, dataCol, Bytes.toBytes(data));
	}
	
	public byte[] getDataCol() {
		return dataCol;
	}
}
