package com.brofan.service.feature.shop;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.brofan.table.ShopDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.entity.Score;

public class MeanStdDev extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(MeanStdDev.class);

	public static class MeanStdDevMapper extends
			TableMapper<Text, SortedMapWritable> {

		private IntWritable rate = new IntWritable();
		private Text shopId = new Text();
		private static final LongWritable ONE = new LongWritable(1);
		private String[] scoreNames;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			scoreNames = conf.getStrings("job.MeanStdDev.scoreNames");
		}

		@Override
		public void map(ImmutableBytesWritable row, Result result, Context context)
				throws IOException, InterruptedException {

			for (int i = 0; i< scoreNames.length; i++) {
				try {
					byte[] b = result.getValue(ShopDataTable.FAM_NAME, Score.getDataCol(scoreNames[i]));
					rate.set( Bytes.toInt(b) );
				} catch (IllegalArgumentException ie) {
					log.error(ie.getMessage(), ie);
					return;
				}

				SortedMapWritable outRate = new SortedMapWritable();
				outRate.put(rate, ONE);

				String[] sDataKey = Bytes.toString(result.getRow()).split("_");

				// Build OutKey String
				StringBuffer outKey = new StringBuffer(sDataKey[0]);
				outKey.append("_");
				outKey.append(scoreNames[i]);

				shopId.set(outKey.toString());

				context.write(shopId, outRate);
			}
		}
	}

	public static class MeanStdDevCombiner extends
			Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {
		//TableReducer<Text, SortedMapWritable, Text> {

		@Override
		@SuppressWarnings("rawtypes")
		protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {

			SortedMapWritable outValue = new SortedMapWritable();
			for (SortedMapWritable v : values) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					LongWritable count = (LongWritable) outValue.get(entry.getKey());

					if (count != null) {
						count.set(count.get() + ((LongWritable) entry.getValue()).get());
						outValue.put(entry.getKey(), count);
					} else {
						outValue.put(entry.getKey(), new LongWritable(
								((LongWritable)entry.getValue()).get()));
					}
				}
			}
			context.write(key, outValue);
		}
	}

	public static class MeanStdDevReducer extends
			TableReducer<Text, SortedMapWritable, Put> {

		private TreeMap<Integer, Long> rateCounts = new TreeMap<Integer, Long>();

		@Override
		@SuppressWarnings("rawtypes")
		public void reduce(Text key, Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			long total = 0;
			rateCounts.clear();

			for (SortedMapWritable v : values) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					int rate = ((IntWritable) entry.getKey()).get();
					long count = ((LongWritable) entry.getValue()).get();

					total += count;
					sum += rate * count;

					Long storedCount = rateCounts.get(rate);
					if (storedCount == null) {
						rateCounts.put(rate, count);
					} else {
						rateCounts.put(rate, storedCount + count);
					}
				}
			}

			// calculate mean

			float mean = sum / total;

			String[] inKey = key.toString().split("_");
			String shopId = inKey[0];
			String scoreName = inKey[1];

			Put put = ShopFeatureTable.getPut(shopId);
			ShopFeatureTable.putMean(put, scoreName, mean);

			// calculate standard deviation

			float sumOfSquares = 0.0f;
			for (Entry<Integer, Long> entry : rateCounts.entrySet()) {
				sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
			}

			float sd = (float)Math.sqrt(sumOfSquares / (total -1));

			ShopFeatureTable.putSD(put, scoreName, sd);
			context.write(null, put);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.setStrings("job.MeanStdDev.scoreNames", Score.getAllScore());

		Job job = configureJob(conf);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		return 0;
	}

	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "MeanStdDev");
		job.setJarByClass(MeanStdDev.class);

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ShopDataTable.TAB_NAME),	// input table
				scan, 												// Scan instance to control CF and attribute selection
				MeanStdDevMapper.class,							// mapper class
				Text.class,											// mapper output key
				SortedMapWritable.class,						// mapper output value
				job);
		job.setCombinerClass(MeanStdDevCombiner.class);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(ShopFeatureTable.TAB_NAME),// output table
				MeanStdDevReducer.class,						// reducer class
				job);
		return job;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MeanStdDev(), args);
		System.exit(res);
	}
}