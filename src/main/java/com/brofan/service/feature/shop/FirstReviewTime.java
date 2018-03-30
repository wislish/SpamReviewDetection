package com.brofan.service.feature.shop;

import java.io.IOException;
//import java.text.Format;
//import java.text.SimpleDateFormat;
//import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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

import com.brofan.table.ShopDataTable;
import com.brofan.table.ShopFeatureTable;

public class FirstReviewTime extends Configured implements Tool {
	
	public static class FirstReviewMapper
			extends TableMapper<Text, LongWritable> {
		
		LongWritable reviewtime = new LongWritable();
		
		@Override
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {

			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String shopid = rDataKey[0];
			reviewtime.set(Long.valueOf(rDataKey[1]).longValue());
			context.write(new Text(shopid), reviewtime);

		}
	}
	
	public static class FirstReviewCombiner
			extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			
			Long earlistTime = Long.MAX_VALUE;
			Iterator<LongWritable> it = values.iterator();
			
			while(it.hasNext()) {
				Long eachvalue = it.next().get();
				if(eachvalue < earlistTime){
					earlistTime = eachvalue;
				}
			}
			
			context.write(key, new LongWritable(earlistTime));
		}
	}
	
	public static class FirstReviewReducer
			extends TableReducer<Text, LongWritable, Put> {
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			
			Long earlistTime = Long.MAX_VALUE;
			Iterator<LongWritable> value = values.iterator();
			
			while(value.hasNext()){
				Long eachvalue = value.next().get();
				if(eachvalue < earlistTime){
					earlistTime = eachvalue;
				}
			}
//			Date date = new Date(earlistTime);
//		   Format format = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss");
//		   String s = format.format(date);
//		   System.out.println(s);
		   
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(
					ShopFeatureTable.FAM_NAME,
					ShopFeatureTable.FR_COL,
					Bytes.toBytes(earlistTime) );
			context.write(null, put);
		}

	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		
		Job job = new Job(conf, "FirstTimeReview");
		job.setJarByClass(FirstReviewTime.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setFilter(new KeyOnlyFilter());
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ShopDataTable.TAB_NAME),	// input table
				scan,													// Scan instance to control CF and attribute selection
				FirstReviewMapper.class,						// mapper class
				Text.class,											// mapper output key
				LongWritable.class,								// mapper output value
				job);
		job.setCombinerClass(FirstReviewCombiner.class);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(ShopFeatureTable.TAB_NAME),// output table
				FirstReviewReducer.class, 						// reducer class
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
		int res = ToolRunner.run(new FirstReviewTime(), args);
		System.exit(res);
	}
}
