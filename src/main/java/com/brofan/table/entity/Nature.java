package com.brofan.table.entity;

import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.table.entity.type.NatureType;

public final class Nature {
	private static NatureType natures[];
	static {
		String[] natureArray = {"w","ns","nr","n","v","vn","t","m","a","d"};
		
		natures = new NatureType[natureArray.length];
		for (int idx = 0; idx < natureArray.length; ++idx) {
			natures[idx] = new NatureType(natureArray[idx]);
		}
	}
	
	private Nature() {
	}
	
	public static void putData(Put put, byte[] family, HashMap<String, Double> statistics) {
		for (int idx = 0; idx < natures.length; ++idx) {
			NatureType n = natures[idx];
			put.add(
				family,
				n.getColName(),
				Bytes.toBytes( statistics.get(n.getStr()) != null ? statistics.get(n.getStr()) : 0 ));
		}
	}
}
