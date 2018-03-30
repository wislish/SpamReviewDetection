package com.brofan.service.feature.shop;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.ReviewDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;

public class SRr extends Configured implements Tool {
	public static class MapperClass 
		extends TableMapper<Text, LongWritable> {
	
		private LongWritable ONE = new LongWritable(1);
		private Hashtable<String, Boolean> SRUser = new Hashtable<String, Boolean>();
	
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			HTable uFtrTable = new HTable(conf, UserFeatureTable.TAB_NAME);
			
			Scan scan = new Scan();
			scan.setCaching(1000);	// 面向行优化
			scan.setCacheBlocks(false);
			
			scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.SR_COL);
			
			ResultScanner rs = uFtrTable.getScanner(scan);
			try {
				for (Result r : rs) {
					String key = Bytes.toString(r.getRow());
					Boolean value = Bytes.toBoolean(
							r.getValue(
									UserFeatureTable.FAM_NAME,
									UserFeatureTable.SR_COL) );
					
					SRUser.put(key, value);
				}
			} finally {
				rs.close();
			}
			uFtrTable.close();
		}
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
				throws IOException, InterruptedException {
			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String userid = rDataKey[0];
			String shopid = rDataKey[1];
			
			if (SRUser.get(userid)) {
				context.write(new Text(shopid), ONE);
			}
		}
	}
	
	// TODO: Add CombinerClass to reduce network traffic
	public static class CombinerClass
		extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<LongWritable> i = values.iterator();
			long numSRr = 0;
			while (i.hasNext()) {
				numSRr += i.next().get();
			}
			
			context.write(key, new LongWritable(numSRr));
		}
	}
		
	
	public static class ReducerClass
		extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
	
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<LongWritable> i = values.iterator();
			long numSRr = 0;
			while (i.hasNext()) {
				numSRr += i.next().get();
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(
					UserFeatureTable.FAM_NAME,
					UserFeatureTable.SR_COL,
					Bytes.toBytes(numSRr));
			
			context.write(null, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.setFilter(new KeyOnlyFilter());
		
		Job job = new Job(conf, "SRr");
		job.setJarByClass(SRr.class);
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewDataTable.TAB_NAME),	// table name
				scan, 												// Scan instance to control CF and attribute selection
				MapperClass.class,								// mapper class
				Text.class,											// mapper output key
				LongWritable.class,								// mapper output value
				job);
		job.setCombinerClass(CombinerClass.class);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(ShopFeatureTable.TAB_NAME),
				ReducerClass.class,
				job); 
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
		
		int res = ToolRunner.run(new SRr(), args);
		System.exit(res);
	}
}
