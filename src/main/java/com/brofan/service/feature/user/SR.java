package com.brofan.service.feature.user;

import java.io.IOException;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.ReviewDataTable;
import com.brofan.table.UserFeatureTable;

public class SR extends Configured implements Tool {
	
	public static class MapperClass 
		extends TableMapper<Text, Text> {
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
				throws IOException, InterruptedException {
			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String userid = rDataKey[0];
			String shopid = rDataKey[1];
			
			context.write(new Text(userid), new Text(shopid));
		}
	}
	
	public static class CombinerClass
		extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			boolean isSim = true;
			String firstValue = new String();
			
			for(Iterator<Text> i = values.iterator(); i.hasNext(); ) {
				String value = i.next().toString();
				if (firstValue.isEmpty()) {	// first time
					firstValue = value;
				} else if ( !firstValue.equals(value) ){
					isSim = false;
				}
			}
			
			if (isSim) {
				context.write(key, new Text(firstValue));
			} else {
				context.write(key, new Text());
			}
		}
	}
	
	// Same Keys are guaranteed sent to same Reducer
	public static class ReducerClass
		extends TableReducer<Text, Text, ImmutableBytesWritable> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			boolean isSim = true;
			String firstValue = new String();
			
			for(Iterator<Text> i = values.iterator(); i.hasNext(); ) {
				String value = i.next().toString();
				if (!value.isEmpty()) {	// may be SR
					if (firstValue.isEmpty()) {
						firstValue = value;
					} else if ( !firstValue.equals(value) ){
						isSim = false;
						break;
					}
				} else {						// already checked by combiner not SR
					isSim = false;
					break;
				}
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(
					UserFeatureTable.FAM_NAME,
					UserFeatureTable.SR_COL,
					Bytes.toBytes(isSim));
			context.write(null, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.setFilter(new KeyOnlyFilter());
		
		Job job = new Job(conf, "SR");
		job.setJarByClass(SR.class);
		//job.setNumReduceTasks(1);
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewDataTable.TAB_NAME),	// table name
				scan, 												// Scan instance to control CF and attribute selection
				MapperClass.class,								// mapper class
				Text.class,											// mapper output key
				Text.class,											// mapper output value
				job);
		job.setCombinerClass(CombinerClass.class);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(UserFeatureTable.TAB_NAME),
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
		int res = ToolRunner.run(new SR(), args);
		System.exit(res);
	}
}
