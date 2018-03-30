package com.brofan.service.classifier.dt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.brofan.table.ReviewFeatureTable;
import com.brofan.table.entity.Score;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

import com.brofan.service.feature.shop.FirstReviewTime;
import com.brofan.service.feature.shop.FirstReviewTime.FirstReviewCombiner;
import com.brofan.service.feature.shop.FirstReviewTime.FirstReviewMapper;
import com.brofan.service.feature.shop.FirstReviewTime.FirstReviewReducer;
import com.brofan.table.ShopDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;

public class TransReliaToHdfs extends Configured implements Tool {

	public static class TransferReliabilityMapper extends
			TableMapper<Text, Text> {

		private Hashtable<String, Double> userReliability = new Hashtable<String, Double>();
		private Hashtable<String, Double> shopReliability = new Hashtable<String, Double>();
		LongWritable reviewtime = new LongWritable();
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf =  context.getConfiguration();

			HTable uFtrTable = new HTable(conf, UserFeatureTable.TAB_NAME);
			HTable sFtrTable = new HTable(conf, ShopFeatureTable.TAB_NAME);
			Scan scan = new Scan();

			// never forget to set cache!!! T_T
			scan.setCaching(1000);
			scan.setCacheBlocks(false);

			scan.addColumn(ShopFeatureTable.FAM_NAME,ShopFeatureTable.CREDIBILITY_COL );
			ResultScanner rs = sFtrTable.getScanner(scan);
			try {
				for (Result r : rs) {
					String key = Bytes.toString(r.getRow());
					byte[] b = r.getValue(ShopFeatureTable.FAM_NAME, ShopFeatureTable.CREDIBILITY_COL);
					double value = Bytes.toDouble(b);

					shopReliability.put(key, value);
				}
			} finally {
				rs.close();
			}
			sFtrTable.close();

			Scan scan2 = new Scan();

			// never forget to set cache!!! T_T
			scan2.setCaching(1000);
			scan2.setCacheBlocks(false);

			scan2.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.CREDIBILITY_COL);

			ResultScanner rs2 = uFtrTable.getScanner(scan2);
			try {
				for (Result r : rs2) {
					String key = Bytes.toString(r.getRow());
					byte[] b = r.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.CREDIBILITY_COL);
					double value = Bytes.toDouble(b);

					userReliability.put(key, value);
				}
			} finally {
				rs2.close();
			}
			uFtrTable.close();

		}
		@Override
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {


			String[] reviewKey = Bytes.toString(result.getRow()).split("_");
			String userid = reviewKey[0];
			String shopid = reviewKey[1];

			StringBuffer output = new StringBuffer();

//			byte[] b = result.getValue(ReviewFeatureTable.FAM_NAME, ReviewFeatureTable);
//			if (values != null) {
//				double a = Bytes.toDouble(values);
//				String s_credi = Double.toString(a);
//				context.write(new Text("S_CRE"),new Text(s_credi));
//			} else if (values2 != null) {
//				double a = Bytes.toDouble(values2);
//				String u_credi = Double.toString(a);
//				context.write(new Text("U_CRE"),new Text(u_credi));			}
			
			//context.write(new Text(Bytes.toString(values)), new Text(Bytes.toString(values2)));
		}
	}

	public static Job configureJob(Configuration conf) throws Exception {

		Job job = new Job(conf, "TransReliaToHdfs");
		job.setJarByClass(TransReliaToHdfs.class);


		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
				ReviewFeatureTable.TAB_NAME);

//		Scan scan2 = new Scan();
//		scan2.setCaching(500);
//		scan2.setCacheBlocks(false);
//		scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
//				ShopFeatureTable.TAB_NAME);
//		scans.add(scan2);

		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewFeatureTable.TAB_NAME),    // input table
				scan,                                                // Scan instance to control CF and attribute selection
				TransferReliabilityMapper.class,                            // mapper class
				Text.class,                                            // mapper output key
				LongWritable.class,                        // mapper output value
				job);
		

		
		job.setNumReduceTasks(0);
		
		Path trainDir = new Path(conf.get("temp.export.path"));
		
		FileOutputFormat.setOutputPath(job, trainDir);
	
//		job.setCombinerClass(FirstReviewCombiner.class);
//		TableMapReduceUtil.initTableReducerJob(
//				Bytes.toString(ShopFeatureTable.TAB_NAME),// output table
//				FirstReviewReducer.class, // reducer class
//				job);

		return job;
	}

	public int run(String[] arg0) throws Exception {

		Job job = configureJob(getConf());

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("project-site.xml");
		
		HadoopUtil.delete(conf, new Path(conf.get("temp.export.path")));
		int res = ToolRunner.run(conf, new TransReliaToHdfs(), args);
		System.exit(res);
	}
}
