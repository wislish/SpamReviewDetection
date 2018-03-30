package com.brofan.table;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.table.entity.Score;

public final class ShopFeatureTable {

	public final static byte[] TAB_NAME = Bytes.toBytes("sFeature");
	public final static byte[] FAM_NAME = Bytes.toBytes("r");
	public final static byte[] SR_COL = Bytes.toBytes("sr");
	public final static byte[] FR_COL = Bytes.toBytes("fr");
	public final static byte[] BST_COL = Bytes.toBytes("bst");
	public final static byte[] DCV_COL = Bytes.toBytes("dcv");
	public final static byte[] RCV_COL = Bytes.toBytes("rcv");
	public final static byte[] SPAM_COL = Bytes.toBytes("spam");
	public final static byte[] CREDIBILITY_COL = Bytes.toBytes("credibility");
	
	private ShopFeatureTable() {
	}
	
	public static Put getPut(String shopId) {
		Put put = new Put(Bytes.toBytes(shopId));
		return put;
	}
	
	public static void putMean(Put put, String name, float value) {
		Score.putMean(put, name, FAM_NAME, value);
	}
	
	public static void putSD(Put put, String name, float value) {
		Score.putSD(put, name, FAM_NAME, value);
	}
}
