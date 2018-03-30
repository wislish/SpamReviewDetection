package com.brofan.service.tester.dataloader;

import java.io.IOException;

import com.brofan.table.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.service.preprocessor.ReviewRecordParser;
import com.brofan.service.preprocessor.entity.Review;
import com.brofan.service.preprocessor.entity.ReviewType;

public class TestDataLoader extends Configured implements Tool {

	public static class TestDataLoaderMapper
		extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
		private ReviewRecordParser parser = new ReviewRecordParser();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			parser.parse(value);
			if (parser.isValidRecord()) {
				Review review = parser.getReview();

				context.write(null, TestDataTable.putData(review));
				
			} else if (parser.isMalformedReview()) {
				System.err.println("Ignoring possibly corrupt input:" + value);
				context.getCounter(ReviewType.MALFORMED).increment(1);
			} else if (parser.isMissingReview()) {
				System.err.println("Ignoring possibly missing input:" + value);
				context.getCounter(ReviewType.MISSING).increment(1);
			}
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		conf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(TestDataTable.TAB_NAME));

		Job job = Job.getInstance(conf, "TestDataLoader");
		job.setJarByClass(TestDataLoader.class);
		job.setMapperClass(TestDataLoaderMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		Path in = new Path(conf.get("data.unlabled.path"));
		FileInputFormat.setInputPaths(job, in);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		return job;
	}

	public int run(String[] arg0) throws Exception {
		
		Configuration conf = getConf();

		TableManager.setup(conf, TestDataTable.TAB_NAME, TestDataTable.FAM_NAME);
		
		Job job = configureJob(conf);
		
		if (!job.waitForCompletion(true)) {
			return -1;
		}
		
		// Output Result
		Counters counters = job.getCounters();
		long total = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
				"MAP_INPUT_RECORDS").getValue();
		long malform = counters.findCounter(ReviewType.MALFORMED).getValue();
		long missing = counters.findCounter(ReviewType.MISSING).getValue();
		
		System.out.println("Total:" + total);
		System.out.println("MalFormed:" + malform);
		System.out.println("Missing:" + missing);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("project-site.xml");
		
		int res = ToolRunner.run(conf, new TestDataLoader(), args);
		System.exit(res);
	}
}
