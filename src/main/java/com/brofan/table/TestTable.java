package com.brofan.table;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.service.util.RowKeyBuilder;
import com.brofan.table.entity.Score;
import com.brofan.table.entity.Text;

public final class TestTable {
	
	public final static byte[] TAB_NAME = Bytes.toBytes("test");
	public final static byte[] FAM_NAME = Bytes.toBytes("r");
	public final static byte[] STARD_COL = Bytes.toBytes("stard");
	public final static byte[] SCORED_COL = Bytes.toBytes("scored");
	public final static byte[] LENGTH_COL = Bytes.toBytes("len");
	
	private TestTable() {
	}
	
	protected static byte[] buildRowkey(Review review) {
		RowKeyBuilder rb = new RowKeyBuilder(review.getUpdatetime());
		rb.append(review.getUserid());
		rb.append(review.getShopid());
		
		return rb.build();
	}
	
	public static Put putData(Review review) { 
		Put put = new Put(buildRowkey(review));
		
		// put score data
		Score.putData(put, FAM_NAME, review);
		
		Text.putData(put, FAM_NAME, review);
		
		return put;
	}
}
