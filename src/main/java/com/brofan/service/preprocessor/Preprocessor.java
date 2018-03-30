package com.brofan.service.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.service.preprocessor.entity.Review;
import com.brofan.service.preprocessor.entity.ReviewType;
import com.brofan.table.ReviewDataTable;
import com.brofan.table.ShopDataTable;
import com.brofan.table.TableManager;

public class Preprocessor extends Configured implements Tool {
	
	public static class MapperClass
		extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		
		private ReviewRecordParser parser = new ReviewRecordParser();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

				parser.parse(value);
			if (parser.isValidRecord()) {
				Review review = parser.getReview();
        		
        		ImmutableBytesWritable putTable = new ImmutableBytesWritable(ReviewDataTable.TAB_NAME);
        		context.write(putTable, ReviewDataTable.putData(review));
        		
        		// write to the second table sData(store data)
        		putTable = new ImmutableBytesWritable(ShopDataTable.TAB_NAME);
        		context.write(putTable, ShopDataTable.putData(review));
        		
			} else if (parser.isMalformedReview()) {
				System.err.println("Ignoring possibly corrupt input:" + value);
				context.getCounter(ReviewType.MALFORMED).increment(1);
			} else if (parser.isMissingReview()) {
				System.err.println("Ignoring possibly missing input:" + value);
				context.getCounter(ReviewType.MISSING).increment(1);
			}
		}
	}

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		TableManager.setup(conf, ReviewDataTable.TAB_NAME, ReviewDataTable.FAM_NAME);
		TableManager.setup(conf, ShopDataTable.TAB_NAME, ShopDataTable.FAM_NAME);
		
		Job job = Job.getInstance(conf, "Preprocessor");
		job.setJarByClass(Preprocessor.class);
		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		Path in = new Path(conf.get("data.labled.path"));
		FileInputFormat.setInputPaths(job, in);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		
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
		
		int res = ToolRunner.run(conf, new Preprocessor(), args);
		System.exit(res);
	}
}