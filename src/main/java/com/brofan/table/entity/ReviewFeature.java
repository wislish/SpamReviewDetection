package com.brofan.table.entity;

import org.apache.hadoop.hbase.util.Bytes;

public final class ReviewFeature {
	public final static byte[] STARD_COL = Bytes.toBytes("stard");
	public final static byte[] SCORED_COL = Bytes.toBytes("scored");
	public final static byte[] LENGTH_COL = Bytes.toBytes("len");
	
	private ReviewFeature() {
	}
}
