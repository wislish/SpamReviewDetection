package com.brofan.service.feature.user;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

import com.brofan.service.feature.shop.FirstReviewTime;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;

public class ETF extends Configured implements Tool {
	public static class ETFMapper
			extends Mapper<Text, Text, Text, LongWritable> {
		
		private Hashtable<String, Long> shopFirstRTime = new Hashtable<String, Long>();
		
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			HTable sFtrTable = new HTable(conf, ShopFeatureTable.TAB_NAME);
			
			Scan scan = new Scan();
			// never forget to set cache!!! T_T
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			
			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.FR_COL);
			
			ResultScanner rs = sFtrTable.getScanner(scan);
			try {
				for (Result r : rs) {
					String key = Bytes.toString(r.getRow());
					Long value = Bytes.toLong(
							r.getValue(
									ShopFeatureTable.FAM_NAME,
									ShopFeatureTable.FR_COL) );
					
					shopFirstRTime.put(key, value);
				}
			} finally {
				rs.close();
			}
			sFtrTable.close();
		}
		
		@Override
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			// key	-> uid
			// value -> sid	lastTime
			
			String[] values = value.toString().split("\t");
			String shopId = values[0];
			String userLastRTime = values[1];
			
			Long diff = Long.parseLong(userLastRTime) - shopFirstRTime.get(shopId);
			
			context.write(key, new LongWritable(diff));
			//System.out.println("用户最晚评论商家时间与商家最早评论时间相差" + diff/(3600*24*1000) + "天");
		}
	}
	
	public static class ETFReducer
			extends TableReducer<Text, LongWritable, Put> {
		
		final static long HALF_YEAR_SECONDS = 15552000L;
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			
			float etf = 0f;
			
			for (LongWritable value : values) {
				
				if (value.get()/1000 > HALF_YEAR_SECONDS) {
					etf = Math.max(0, etf);
				} else {
					etf = Math.max(etf, 1-(float)value.get()/(HALF_YEAR_SECONDS*1000));
				}
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(
				UserFeatureTable.FAM_NAME,
				UserFeatureTable.ETF_COL,
				Bytes.toBytes(etf));
			context.write(null, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "ETFJob");
		job.setJarByClass(ETF.class);
		
		job.setMapperClass(ETFMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(UserFeatureTable.TAB_NAME),// output table
				ETFReducer.class, 								// reducer class
				job);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		KeyValueTextInputFormat.addInputPath(job, new Path(conf.get("temp.etf.path")));
		
		return job;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		// Submit job and immediately return, rather than waiting for completion
		Job shopFirstRJob = FirstReviewTime.configureJob(conf);
		shopFirstRJob.submit();
		
		Job userShopLastRJob = LastReviewTime.configureJob(conf);
		userShopLastRJob.submit();
		
		// while both jobs are not finished, sleep
		while (!shopFirstRJob.isComplete() || !userShopLastRJob.isComplete()) {
			Thread.sleep(5000);
		}
		
		int code = 0;
		// if both jobs succeed, continue
		if (shopFirstRJob.isSuccessful() && userShopLastRJob.isSuccessful()) {
			Job ETFJob = ETF.configureJob(conf);
			
			code = ETFJob.waitForCompletion(true) ? 0 : 1;
		} else {	// one of the jobs failed
			code = 1;
		}
		
		// Clean up the intermediate output
		if (userShopLastRJob.isSuccessful()) {
			HadoopUtil.delete(conf, new Path(conf.get("temp.etf.path")));
		}
		
		return code;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("project-site.xml");
		
		HadoopUtil.delete(conf, new Path(conf.get("temp.etf.path")));
		int res = ToolRunner.run(conf, new ETF(), args);
		System.exit(res);
	}
}
