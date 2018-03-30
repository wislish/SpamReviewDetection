package com.brofan.service.feature.shop;

import java.io.IOException;

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

import com.brofan.table.ShopFeatureTable;
import com.brofan.table.entity.Score;

public class RCV extends Configured implements Tool {
	
	public static class RCVMapper
			extends TableMapper<ImmutableBytesWritable, Put> {
		
		@Override
		public void map(ImmutableBytesWritable row, Result result, Context context)
				throws IOException, InterruptedException {
			
			byte[] b = result.getValue(ShopFeatureTable.FAM_NAME, Score.getMeanCol("star"));
			float mean = Bytes.toFloat(b);
			
			b = result.getValue(ShopFeatureTable.FAM_NAME, Score.getSdCol("star"));
			float sd = Bytes.toFloat(b);
			
			Put put = new Put(row.get());
			put.add(
				ShopFeatureTable.FAM_NAME,
				ShopFeatureTable.RCV_COL,
				Bytes.toBytes(sd/mean));
			
			context.write(row, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "RCV");
		job.setJarByClass(RCV.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.addColumn(ShopFeatureTable.FAM_NAME, Score.getMeanCol("star"));
		scan.addColumn(ShopFeatureTable.FAM_NAME, Score.getSdCol("star"));
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ShopFeatureTable.TAB_NAME),// input table
				scan, 												// Scan instance to control CF and attribute selection
				RCVMapper.class,									// mapper class
				null,													// mapper output key
				null,													// mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(ShopFeatureTable.TAB_NAME),// output table
				null,            									// reducer class
				job);
		job.setNumReduceTasks(0);
		
		return job;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		Job job = configureJob(conf);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
		    throw new IOException("error with job!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new RCV(), args);
		System.exit(res);
	}
}
