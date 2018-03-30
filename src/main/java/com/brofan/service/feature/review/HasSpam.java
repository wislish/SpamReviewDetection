package com.brofan.service.feature.review;

import java.io.IOException;

import com.brofan.table.ReviewFeatureTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.ReviewDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;
import com.brofan.table.entity.LogReason;
import com.brofan.table.entity.type.LogReasonType;

public class HasSpam extends Configured implements Tool {
	
	public static class HasSpamMapper
			extends TableMapper<Text, NullWritable> {

		@Override
		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
				throws IOException, InterruptedException {


			byte[] b = result.getValue(ReviewDataTable.FAM_NAME, LogReason.LOG_REASON_COL);
			int type = Bytes.toInt(b);
			LogReasonType lrt = LogReasonType.valueOf(type);


			if (lrt == LogReasonType.HYPED) {
				String[] rDataKey = Bytes.toString(result.getRow()).split("_");

				String uid = rDataKey[0];
				String sid = rDataKey[1];
				context.write(new Text("u" + uid), NullWritable.get());
				context.write(new Text("s" + sid), NullWritable.get());
				context.write(new Text("r" + Bytes.toString(result.getRow())), NullWritable.get());
			}
		}
	}
	
	public static class HasSpamCombiner
			extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		}
	}
	
	public static class HasSpamReducer
			extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			String sKey = key.toString();
			ImmutableBytesWritable retkey;
			Put put;
			
			if (sKey.startsWith("u")) {									// represent userid
				
				retkey = new ImmutableBytesWritable(UserFeatureTable.TAB_NAME);
				put = new Put(Bytes.toBytes(sKey.substring(1)));		// remove the starting u
				put.add(
						UserFeatureTable.FAM_NAME,
						UserFeatureTable.SPAM_COL,
						Bytes.toBytes(false));
				
				
			} else if (sKey.startsWith("s")){							// represent shopid
				
				retkey = new ImmutableBytesWritable(ShopFeatureTable.TAB_NAME);
				put = new Put(Bytes.toBytes(sKey.substring(1)));		// remove the starting u
				put.add(
						ShopFeatureTable.FAM_NAME,
						ShopFeatureTable.SPAM_COL,
						Bytes.toBytes(false));

			} else {

				retkey = new ImmutableBytesWritable(ReviewFeatureTable.TAB_NAME);
//				System.out.println("review key "+sKey);
				put = new Put(Bytes.toBytes(sKey.substring(1)));		// remove the starting r
				put.add(
						ReviewFeatureTable.FAM_NAME,
						ReviewFeatureTable.SPAM_NAME,
						Bytes.toBytes(false));
			}
			context.write(retkey, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "HasSpam");
		job.setJarByClass(HasSpam.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewDataTable.TAB_NAME),	// input table
				scan, 												// Scan instance to control CF and attribute selection
				HasSpamMapper.class,								// mapper class
				Text.class,											// mapper output key
				NullWritable.class,								// mapper output value
				job);
		job.setCombinerClass(HasSpamCombiner.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setReducerClass(HasSpamReducer.class);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration());
		
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
		int res = ToolRunner.run(HBaseConfiguration.create(), new HasSpam(), args);
		System.exit(res);
	}
}
