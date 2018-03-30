package com.brofan.table;

import org.apache.hadoop.hbase.util.Bytes;

public final class ReviewFeatureTable {
	public final static byte[] TAB_NAME = Bytes.toBytes("rFeature");
	public final static byte[] FAM_NAME = Bytes.toBytes("r");
	public final static byte[] SPAM_NAME = Bytes.toBytes("spam");
	public final static byte[] STARD_COL = Bytes.toBytes("stard");
	public final static byte[] SCORED_COL = Bytes.toBytes("scored");
	public final static byte[] LENGTH_COL = Bytes.toBytes("len");

	private ReviewFeatureTable() {
	}
	
}
