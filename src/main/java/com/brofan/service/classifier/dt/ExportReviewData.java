package com.brofan.service.classifier.dt;

import java.io.IOException;
import java.util.Hashtable;

import com.brofan.table.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

import com.brofan.service.feature.user.RD;
import com.brofan.service.feature.user.RD.CombinerClass;
import com.brofan.service.feature.user.RD.MapperClass;
import com.brofan.service.feature.user.RD.ReducerClass;
import com.brofan.service.feature.user.util.AvgWritable;
import com.brofan.table.entity.Score;

public class ExportReviewData extends Configured implements Tool {

	public static class MapperClass extends TableMapper<Text, Text> {

		private Hashtable<String, Double> userReliability = new Hashtable<String, Double>();
		private Hashtable<String, Double> shopReliability = new Hashtable<String, Double>();

		private int type =1;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			byte[] table=null;
			String tname = conf.get("tname");
			if (tname.equals("TestTable")){
				type=0;
			}
			HTable uFtrTable = new HTable(conf, UserFeatureTable.TAB_NAME);
			HTable sFtrTable = new HTable(conf, ShopFeatureTable.TAB_NAME);
			Scan scan = new Scan();

			// never forget to set cache!!! T_T
			scan.setCaching(1000);
			scan.setCacheBlocks(false);

			scan.addColumn(ShopFeatureTable.FAM_NAME, ShopFeatureTable.CREDIBILITY_COL);
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

		public void map(ImmutableBytesWritable rowkey, Result result,
				Context context) throws IOException, InterruptedException {

			String[] rFeatureKey = Bytes.toString(result.getRow()).split("_");
			String reviewFeature = "";

			String userid = rFeatureKey[0];
			String shopid = rFeatureKey[1];
			int isSpam=0;
			int num=0;
			if ( userReliability.get(userid)!=null && shopReliability.get(shopid)!=null) {
				reviewFeature = reviewFeature + userReliability.get(userid) + " "
						+ shopReliability.get(shopid) + " ";
				for (KeyValue keyValue : result.list()) {

					String qualifier = Bytes.toString(keyValue.getQualifier());
//					System.out.println(num + " Qualifier " + qualifier);
					if (qualifier.equals("len")) {
						int value = Bytes.toInt(keyValue.getValue());
						reviewFeature = reviewFeature + value + " ";
					} else if (qualifier.equals("spam")) {
						num--;
						isSpam = 1;
					} else {
						float value = Bytes.toFloat(keyValue.getValue());
						reviewFeature = reviewFeature + value + " ";
					}
					num++;
				}
				if (num!=13){
					System.out.println("missing :"+reviewFeature+" "+num);
				}
				if (type==1) {
					if (isSpam == 0) {
						reviewFeature = reviewFeature + "0";
					} else {
						reviewFeature = reviewFeature + "1";
					}
				}else{
					reviewFeature = reviewFeature + "0";
				}
//			System.out.println(reviewFeature);
				context.write(null, new Text(reviewFeature));
			}
		}
	}

	public static Job configureJob(Configuration conf) throws Exception {

		Job job = new Job(conf, "ExportReview");
		job.setJarByClass(ExportReviewData.class);

		byte[] table=null;
		String tname = conf.get("tname");
		if (tname.equals("TestTable")){
			table = TestTable.TAB_NAME;
		}else{
			table = ReviewFeatureTable.TAB_NAME;
		}

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
				table, // table name
				scan, // Scan instance to control CF and attribute selection
				MapperClass.class, // mapper class
				null, // mapper output key
				Text.class, // mapper output value
				job);

		job.setNumReduceTasks(0);

		Path exportDir = new Path(conf.get("temp.export.path"));

		FileOutputFormat.setOutputPath(job, exportDir);
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
		int res = ToolRunner.run(conf, new ExportReviewData(), args);
		System.exit(res);
	}
}
