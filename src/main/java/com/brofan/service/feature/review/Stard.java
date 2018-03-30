package com.brofan.service.feature.review;

import java.io.IOException;
import java.util.Hashtable;

import com.brofan.table.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.entity.ReviewFeature;
import com.brofan.table.entity.Score;

public class Stard extends Configured implements Tool {
	public static class StardMapper
			extends TableMapper<ImmutableBytesWritable, Put> {
		

		private Hashtable<String, Float> shopAvgStar = new Hashtable<String, Float>();
		private byte[] table;
		private byte[] family;
		private byte[] qualifier;
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf =  context.getConfiguration();
			String tname = conf.get("tname");
			if (tname.equals("TestTable")){
				table = TestTable.TAB_NAME;
				family = TestTable.FAM_NAME;
				qualifier=TestTable.STARD_COL;
			}else{
				table = ReviewFeatureTable.TAB_NAME;
				family = ReviewFeatureTable.FAM_NAME;
				qualifier=ReviewFeatureTable.STARD_COL;
			}
			byte[] meanStarCol = Score.getMeanCol("star");
			
			HTable sFtrTable = new HTable(conf, ShopFeatureTable.TAB_NAME);
			Scan scan = new Scan();
			
			// never forget to set cache!!! T_T
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			
			scan.addColumn(ShopFeatureTable.FAM_NAME, meanStarCol);
			ResultScanner rs = sFtrTable.getScanner(scan);
			try {
				for (Result r : rs) {
					String key = Bytes.toString(r.getRow());
					byte[] b = r.getValue(ShopFeatureTable.FAM_NAME, meanStarCol);
					float value = Bytes.toFloat(b);
					
					shopAvgStar.put(key, value);
				}
			} finally {
				rs.close();
			}
			sFtrTable.close();
			
		}
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
				throws IOException, InterruptedException {
			byte[] b = result.getValue(family, Score.getDataCol("star"));
			int star = Bytes.toInt(b);
			
			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String shopid = rDataKey[1];
			Float avgStar = shopAvgStar.get(shopid);
			if (avgStar != null) {
				float diff = (float)star - avgStar;
				
				Put put = new Put(result.getRow());
				put.add(
					family,
					qualifier,
					Bytes.toBytes(diff/4));
					
				context.write(null, put);
			}
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "Stard");
		job.setJarByClass(Stard.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		byte[] inputtable=null;
		byte[] outputtable=null;
		String tname = conf.get("tname");
		if (tname.equals("TestTable")){
			inputtable = TestDataTable.TAB_NAME;
			outputtable = TestTable.TAB_NAME;
		}else{
			inputtable = ReviewDataTable.TAB_NAME;
			outputtable = ReviewFeatureTable.TAB_NAME;
		}


		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(inputtable),	// input table
				scan, 												// Scan instance to control CF and attribute selection
				StardMapper.class,								// mapper class
				ImmutableBytesWritable.class,					// mapper output key
				Put.class,											// mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(outputtable),
				null,
				job);
		job.setNumReduceTasks(0);
		return job;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = configureJob(conf);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(HBaseConfiguration.create(), new Stard(), args);
		System.exit(res);
	}
}
