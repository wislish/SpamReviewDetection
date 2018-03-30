package com.brofan.service.feature.review;

import java.io.IOException;

import com.brofan.table.TestDataTable;
import com.brofan.table.TestTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.ReviewDataTable;
import com.brofan.table.ReviewFeatureTable;
import com.brofan.table.entity.ReviewFeature;
import com.brofan.table.entity.Score;

public class Scored extends Configured implements Tool {
	public static class ScoredMapper
			extends TableMapper<ImmutableBytesWritable, Put> {

		private byte[] table;
		private byte[] family;
		private byte[] qualifier;

		public void setup(Context context) {

			Configuration conf = context.getConfiguration();
			String tname = conf.get("tname");
			if (tname.equals("TestTable")){
				table = TestTable.TAB_NAME;
				family = TestTable.FAM_NAME;
				qualifier=TestTable.SCORED_COL;
			}else{
				table = ReviewFeatureTable.TAB_NAME;
				family = ReviewFeatureTable.FAM_NAME;
				qualifier=ReviewFeatureTable.SCORED_COL;
			}
		}
		@Override
		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
				throws IOException, InterruptedException {
			
			byte[] b = result.getValue(family, Score.getDataCol("score1"));
			int score1 = Bytes.toInt(b);
			
			b = result.getValue(family, Score.getDataCol("score2"));
			int score2 = Bytes.toInt(b);
			
			b = result.getValue(family, Score.getDataCol("score3"));
			int score3 = Bytes.toInt(b);
			
			double mean = (score1 + score2 + score3) / 3;
			double stdDev = Math.sqrt(((score1-mean)*(score1-mean)+(score2-mean)*(score2-mean)+(score3-mean)*(score3-mean))/3);
			
			Put put = new Put(result.getRow());
			put.add(
				family, qualifier,
				Bytes.toBytes(stdDev));
			
			context.write(null, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "Scored");
		job.setJarByClass(Scored.class);
		
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
				ScoredMapper.class,								// mapper class
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
		int res = ToolRunner.run(HBaseConfiguration.create(), new Scored(), args);
		System.exit(res);
	}
}
