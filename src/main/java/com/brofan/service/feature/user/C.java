package com.brofan.service.feature.user;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

import com.brofan.table.ReviewDataTable;
import com.brofan.table.UserFeatureTable;
import com.brofan.table.entity.Score;

public class C extends Configured implements Tool {
	public static class CCalMapper
			extends TableMapper<Text, IntWritable> {
		
		private IntWritable star = new IntWritable();
		
		@Override
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {

			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String userid = rDataKey[0];
			String shopid = rDataKey[1];
			String key = userid + "_" + shopid;
			
			byte[] b = result.getValue(ReviewDataTable.FAM_NAME, Score.getDataCol("star"));
			star.set(Bytes.toInt(b));
			context.write(new Text(key), star);
		}
	}
	
	public static class CCalReducer
			extends Reducer<Text, IntWritable, Text, FloatWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			float c = 0;
			
			int total = 0;
			float square = 0;
			float sum = 0;
			
			for (IntWritable value : values) {
				total += 1;
				square += value.get() * value.get();
				sum += value.get();
			}
			
			if (total != 1) {
				float mean = sum / total;
				float sd = (float)Math.sqrt( square / total - mean * mean );

				if (mean == 0) {
					c = total;
				} else {
					c = total * (1 - sd / mean);
				}

				// key -> uid
				String[] splits = key.toString().split("_");
				Text newkey = new Text(splits[0]);
				
				// value -> c for user and shop
				context.write(newkey, new FloatWritable(c));
				
			} else {
				// do nothing
				// user only review this shop once!
			}
		}
	}
	
	public static class CSumMapper
			extends Mapper<Text, Text, Text, FloatWritable> {
		
		@Override
		public void map(Text key, Text value,
				Context context) throws IOException, InterruptedException {
			
			context.write(key, new FloatWritable( Float.parseFloat(value.toString()) ));
		}
	}
	
	public static class CSumReducer
			extends TableReducer<Text, FloatWritable, Put> {
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			
			float sum = 0;
			Iterator<FloatWritable> value = values.iterator();
			
			while(value.hasNext()){
				sum += value.next().get();
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(
					UserFeatureTable.FAM_NAME,
					UserFeatureTable.C_COL,
					Bytes.toBytes(sum));
			
			context.write(null, put);
		}
	}
	
	public static Job configureCalJob(Configuration conf) throws Exception {
		
		Job calJob = new Job(conf, "CCalculateJob");
		calJob.setJarByClass(C.class);
		
		Scan scan = new Scan();
		scan.setCaching(500); 			// 1 is the default in Scan, which will be bad for
												// MapReduce jobs
		scan.setCacheBlocks(false); 	// don't set to true for MR jobs
		scan.addColumn(ReviewDataTable.FAM_NAME, Score.getDataCol("star"));
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewDataTable.TAB_NAME), // input table
				scan,													// Scan instance to control CF and attribute selection
				CCalMapper.class, 								// mapper class
				Text.class, 										// mapper output key
				IntWritable.class, 								// mapper output value
				calJob);
		
		calJob.setReducerClass(CCalReducer.class);
		
		calJob.setOutputKeyClass(Text.class);
		calJob.setOutputValueClass(FloatWritable.class);
		
		calJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(calJob, new Path(conf.get("temp.c.path")));
		return calJob;
	}
	
	public static Job configureSumJob(Configuration conf) throws Exception {
		
		Job sumJob = new Job(conf, "CSumJob");
		sumJob.setJarByClass(C.class);
		
		// don't forget reset map key& value class
		sumJob.setMapOutputKeyClass(Text.class);
		sumJob.setMapOutputValueClass(FloatWritable.class);
		sumJob.setMapperClass(CSumMapper.class);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(UserFeatureTable.TAB_NAME),// output table
				CSumReducer.class, 								// reducer class
				sumJob);
		sumJob.setInputFormatClass(KeyValueTextInputFormat.class);
		TextInputFormat.addInputPath(sumJob, new Path(conf.get("temp.c.path")));
		
		return sumJob;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		Job calJob = configureCalJob(conf);
		
		// Execute job and grab exit code
		int code = calJob.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			Job sumJob = configureSumJob(conf);
			
			code = sumJob.waitForCompletion(true) ? 0 : 1;
		}
		
		HadoopUtil.delete(conf, new Path(conf.get("temp.c.path")));
		//FileSystem.get(conf).delete(outputDirIntermediate, true);
		
		return code;
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.addResource("project-site.xml");

		int res = ToolRunner.run(conf, new C(), args);
		System.exit(res);
	}
}
