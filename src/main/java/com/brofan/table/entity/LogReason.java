package com.brofan.table.entity;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.table.entity.type.LogReasonType;

public final class LogReason {
	public static final byte[] LOG_REASON_COL = Bytes.toBytes("logreason");
	
	public static void putData(Put put, byte[] family, LogReasonType reason) {
		put.add(family, LOG_REASON_COL, Bytes.toBytes(reason.getValue()));
	}
}
