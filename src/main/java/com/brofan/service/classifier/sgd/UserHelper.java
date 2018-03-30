package com.brofan.service.classifier.sgd;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;

import com.brofan.service.classifier.sgd.entity.UserFeatures;
import com.brofan.table.UserFeatureTable;

public class UserHelper {
	private static final int FEATURES = 10;
	private static final int CATEGORIES = 2;

	private HConnection connection;
	private ResultScanner resultScanner;
	private Result result;
	private int count;
	private long rowCount;
	public UserHelper() {

		try {
			Configuration conf = HBaseConfiguration.create();
			AggregationClient aggregationClient = new AggregationClient(conf);
			connection = HConnectionManager.createConnection(HBaseConfiguration.create());
			HTableInterface hi = connection.getTable(UserFeatureTable.TAB_NAME);

			Scan scan = new Scan();

			scan.setCaching(500);
			scan.setBatch(5);
			scan.setCacheBlocks(false);

			// add predictors
			scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.RD_COL);
			scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.C_COL);
			scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.SR_COL);
			scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.ETF_COL);
			// TODO： 添加分组可信度

			// add target variable
			scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.SPAM_COL);

			rowCount = aggregationClient.rowCount(UserFeatureTable.TAB_NAME, null, scan);

			resultScanner = hi.getScanner(scan);

			//connection.close();
		} catch (Throwable e) {
			// TODO: a remote or network exception occurs, throw an Exception
			e.printStackTrace();
		}
	}

	public UserFeatures getUserFeatures() {
		UserFeatures uf = new UserFeatures();
		uf.setUid(Bytes.toString(result.getRow()));

		byte[] b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.RD_COL);
		uf.setRD(Bytes.toFloat(b));

		b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.C_COL);
		if (b == null) { 			// C Not Found : means C = 0
			uf.setC(0);
		} else {
			uf.setC(Bytes.toFloat(b));
		}

		b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.ETF_COL);
		uf.setETF(Bytes.toFloat(b));

		b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.SR_COL);
		uf.setSR(Bytes.toBoolean(b));

		b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.SPAM_COL);
		if (b == null) {			// default for not not spam
			uf.setSpam(0);
		} else {						// spam
			uf.setSpam(1);
		}

		return uf;
	}

	public boolean hasNext() throws IOException {
		result = resultScanner.next();
		return result != null ? true : false;
	}

	public long size() {
		return rowCount;
	}

//	public void reset() throws IOException {
//		iterator = resultScanner.iterator();
//	}

	public int getFeaturesCount() {
		return FEATURES;
	}

	public int getCategoriesCount() {
		return CATEGORIES;
	}
}