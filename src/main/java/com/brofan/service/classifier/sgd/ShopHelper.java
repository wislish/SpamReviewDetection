package com.brofan.service.classifier.sgd;

import java.io.IOException;

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

import com.brofan.service.classifier.sgd.entity.ShopFeatures;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;
import com.brofan.table.entity.LogReason;
import com.brofan.table.entity.Score;

public class ShopHelper {
	private static final int FEATURES = 10;
	private static final int CATEGORIES = 2;

	private HConnection connection;
	private ResultScanner resultScanner;
	private Result result;
	private long rowCount;

	public ShopHelper() {

		try {
			Configuration conf = HBaseConfiguration.create();
			AggregationClient aggregationClient = new AggregationClient(conf);
			connection = HConnectionManager.createConnection(HBaseConfiguration.create());
			HTableInterface hi = connection.getTable(ShopFeatureTable.TAB_NAME);

			Scan scan = new Scan();

			scan.setCaching(500);
			scan.setBatch(8);
			scan.setCacheBlocks(false);

			// add predictors
			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.SR_COL);
//			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.BST_COL);
//			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.DCV_COL);
			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.RCV_COL);
			scan.addColumn(ShopFeatureTable.FAM_NAME, Score.getMeanCol("star"));
			scan.addColumn(ShopFeatureTable.FAM_NAME, Score.getMeanCol("score1"));
			scan.addColumn(ShopFeatureTable.FAM_NAME, Score.getMeanCol("score2"));
			scan.addColumn(ShopFeatureTable.FAM_NAME, Score.getMeanCol("score3"));

			// add target variable
			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.SPAM_COL);
			rowCount = aggregationClient.rowCount(ShopFeatureTable.TAB_NAME, null, scan);
			resultScanner = hi.getScanner(scan);


		} catch (Throwable e) {
			// TODO: a remote or network exception occurs, throw an Exception
			e.printStackTrace();
		}
	}

	public ShopFeatures getShopFeatures() {
		ShopFeatures sf = new ShopFeatures();

		sf.setSid(Bytes.toString(result.getRow()));
		byte[] b = result.getValue(ShopFeatureTable.FAM_NAME, ShopFeatureTable.SR_COL);
		if (b == null) { 			// C Not Found : means C = 0
			sf.setSR(0);
		} else {
			sf.setSR(Bytes.toLong(b));
		}


//		b = result.getValue(ShopFeatureTable.FAM_NAME, ShopFeatureTable.BST_COL);
//		sf.setBST(Bytes.toFloat(b));

//		b = result.getValue(ShopFeatureTable.FAM_NAME, ShopFeatureTable.DCV_COL);
//		sf.setDCV(Bytes.toFloat(b));

		b = result.getValue(ShopFeatureTable.FAM_NAME, ShopFeatureTable.RCV_COL);
		sf.setRCV(Bytes.toFloat(b));

		b = result.getValue(ShopFeatureTable.FAM_NAME, Score.getMeanCol("star"));
		sf.setStar(Bytes.toFloat(b));
		System.out.println("STAR " + sf.getStar());
		b = result.getValue(ShopFeatureTable.FAM_NAME, Score.getMeanCol("score1"));
		sf.setScore1(Bytes.toFloat(b));

		b = result.getValue(ShopFeatureTable.FAM_NAME, Score.getMeanCol("score2"));
		sf.setScore2(Bytes.toFloat(b));


		b = result.getValue(ShopFeatureTable.FAM_NAME, Score.getMeanCol("score3"));
		sf.setScore3(Bytes.toFloat(b));

		b = result.getValue(ShopFeatureTable.FAM_NAME, ShopFeatureTable.SPAM_COL);
		if (b == null) {			// default for not not spam
			sf.setSpam(0);
		} else {						// spam
			sf.setSpam(1);
		}

		return sf;
	}

	public boolean hasNext() throws IOException {
		result = resultScanner.next();
		return result != null ? true : false;
	}

	public int getFeaturesCount() {
		return FEATURES;
	}

	public long size() {
		return rowCount;
	}

	public int getCategoriesCount() {
		return CATEGORIES;
	}
}