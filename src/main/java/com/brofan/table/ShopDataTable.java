package com.brofan.table;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.service.util.RowKeyBuilder;
import com.brofan.table.entity.Score;

public final class ShopDataTable {
	
	public final static byte[] TAB_NAME = Bytes.toBytes("sData");
	public final static byte[] FAM_NAME = Bytes.toBytes("r");
	
	private ShopDataTable() {
	}

	public static Put putData(Review review) {
		Put put = new Put(buildRowkey(review));
		
		// put score data
		Score.putData(put, FAM_NAME, review);
		
		return put;
	}
	
	protected static byte[] buildRowkey(Review review) {
		RowKeyBuilder rb = new RowKeyBuilder(review.getUpdatetime());
		rb.append(review.getShopid());
		
		return rb.build();
	}
}
