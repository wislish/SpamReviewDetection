package com.brofan.service.preprocessor;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

// Simple Random Sampling
public class GenTestSet extends Configured implements Tool {
	
	public static class GenTestSetMapper
			extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		private static final Double percentage = 30.0 / 100.0;	// .5% for testSet
		private Random rands = new Random();
		private MultipleOutputs<NullWritable, Text> mos;
		
		@Override
		protected void setup(Context context) {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (rands.nextDouble() < percentage) {	// For Test Set
				// Remove Target Column
//				Text newValue = removeTarget(value);
				Configuration conf = context.getConfiguration();
				mos.write("test", NullWritable.get(), value, conf.get("data.unlabled.path"));
			} else {											// For Train Set
				context.write(NullWritable.get(), value);
			}
		}
		
//		private Text removeTarget(Text text) {
//			String withTarget = text.toString();
//			int tab = withTarget.lastIndexOf("\t");
//			String withoutTarget = withTarget.substring(0, tab);
//			return new Text(withoutTarget);
//		}
		
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
	}

	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		
		HadoopUtil.delete(conf, new Path(conf.get("data.labled.path")));
		HadoopUtil.delete(conf, new Path(conf.get("data.unlabled.path")));
		
		Job job = Job.getInstance(conf, "GenTestSet");
		job.setJarByClass(GenTestSet.class);
		job.setMapperClass(GenTestSetMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		Path inputDir = new Path(conf.get("data.raw.path"));
		Path trainDir = new Path(conf.get("data.labled.path"));
		
		FileInputFormat.setInputPaths(job, inputDir);
		FileOutputFormat.setOutputPath(job, trainDir);
		
		MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, NullWritable.class, Text.class);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		return code;
	}
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("project-site.xml");
		
		int res = ToolRunner.run(conf, new GenTestSet(), args);
		System.exit(res);
	}
}
