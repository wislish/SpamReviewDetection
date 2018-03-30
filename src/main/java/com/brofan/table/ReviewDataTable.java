package com.brofan.table;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.service.util.RowKeyBuilder;
import com.brofan.table.entity.LogReason;
import com.brofan.table.entity.Score;
import com.brofan.table.entity.Text;

public final class ReviewDataTable {
	
	public final static byte[] TAB_NAME = Bytes.toBytes("rData");
	public final static byte[] FAM_NAME = Bytes.toBytes("r");
	
	private ReviewDataTable() {
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
		
		LogReason.putData(put, FAM_NAME, review.getLogreason());
		
		return put;
	}
}
