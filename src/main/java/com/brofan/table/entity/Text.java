package com.brofan.table.entity;

import org.apache.hadoop.hbase.client.Put;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.table.entity.type.TextType;

public final class Text {
	
	// maybe there is a title
	// private static TextType title = new TextType("title");
	private static TextType body = new TextType("body");
	
	private Text() {
	}
	
	public static void putData(Put put, byte[] family, Review review) {
		body.putData(put, family, review.getBody());
	}
	
	public static byte[] getDataCol(String name) {
		if (name.equals("body")) {
			return body.getDataCol();
		} else {
			throw new IllegalArgumentException("getDataCol: " + name + "  not found!");
		}
	}
}
