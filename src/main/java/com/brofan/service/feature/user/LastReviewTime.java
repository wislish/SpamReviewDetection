package com.brofan.service.feature.user;

import java.io.IOException;
//import java.text.Format;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.text.Format;
//import java.text.SimpleDateFormat;
//import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.ReviewDataTable;

public class LastReviewTime extends Configured implements Tool {
	public static class LastReviewMapper
			extends TableMapper<Text, LongWritable> {

		LongWritable reviewtime = new LongWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {

			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String userid = rDataKey[0];
			String shopid = rDataKey[1];
			String key = userid + "\t" + shopid;
			reviewtime.set(Long.valueOf(rDataKey[2]).longValue());
			context.write(new Text(key), reviewtime);

		}
	}
	
	public static class LastReviewCombiner
			extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			
			Long lastTime = Long.MIN_VALUE;
			Iterator<LongWritable> it = values.iterator();
			
			while (it.hasNext()) {
				Long eachvalue = it.next().get();
				if (eachvalue > lastTime) {
					lastTime = eachvalue;
				}
			}
			
			context.write(key, new LongWritable(lastTime));
		}
	}

	public static class LastReviewReducer
			extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			Long lastTime = Long.MIN_VALUE;
			Iterator<LongWritable> value = values.iterator();

			while (value.hasNext()) {
				Long eachvalue = value.next().get();
				if (eachvalue > lastTime) {
					lastTime = eachvalue;
				}
			}
//			Date date = new Date(lastTime);
//		   Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
//		   String s = format.format(date);
//		   System.out.println(s);
			
			context.write(key, new LongWritable(lastTime));
		}

	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		
		Job job = new Job(conf, "LastReviewTime");
		job.setJarByClass(LastReviewTime.class);

		Scan scan = new Scan();
		scan.setCaching(500); 			// 1 is the default in Scan, which will be bad for
												// MapReduce jobs
		scan.setCacheBlocks(false); 	// don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewDataTable.TAB_NAME), // input table
				scan,													// Scan instance to control CF and attribute selection
				LastReviewMapper.class, 						// mapper class
				Text.class, 										// mapper output key
				LongWritable.class, 								// mapper output value
				job);
		
		job.setCombinerClass(LastReviewCombiner.class);
		job.setReducerClass(LastReviewReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String outPath = conf.get("temp.etf.path");
		System.out.println(outPath);
		TextOutputFormat.setOutputPath(job, new Path(outPath));
		
		return job;
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = configureJob(conf);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("project-site.xml");
		
		int res = ToolRunner.run(conf, new LastReviewTime(), args);
		System.exit(res);
	}
}