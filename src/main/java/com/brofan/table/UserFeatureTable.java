package com.brofan.table;

import org.apache.hadoop.hbase.util.Bytes;

public final class UserFeatureTable {

	public final static byte[] TAB_NAME = Bytes.toBytes("uFeature");
	public final static byte[] FAM_NAME = Bytes.toBytes("r");
	public final static byte[] SR_COL = Bytes.toBytes("sr");
	public final static byte[] RD_COL =  Bytes.toBytes("rd");
	public final static byte[] C_COL = Bytes.toBytes("c");
	public final static byte[] ETF_COL = Bytes.toBytes("etf");
	public final static byte[] SPAM_COL = Bytes.toBytes("spam");
	public final static byte[] CREDIBILITY_COL = Bytes.toBytes("credibility");
	
	private UserFeatureTable() {
	}
}
