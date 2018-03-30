package com.brofan.service.feature.user;

import java.io.IOException;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

import com.brofan.service.classifier.sgd.UserHelper;
import com.brofan.service.classifier.sgd.entity.UserFeatures;
import com.brofan.service.feature.user.C.CCalMapper;
import com.brofan.service.feature.user.C.CSumMapper;
import com.brofan.table.ReviewDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;

public class ExportUserFeatures extends Configured implements Tool {

	public static class ExportUserFeaturesMapper extends
			TableMapper<Text, Text> {

		LongWritable reviewtime = new LongWritable();
		UserFeatures uf = new UserFeatures();
		@Override
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			
			String userid = Bytes.toString(result.getRow());
			
			byte[] b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.RD_COL);
			uf.setRD(Bytes.toFloat(b));
			
			b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.C_COL);
			if (b == null) { 			// C Not Found : means C = 0
				uf.setC(0);
			} else {
				uf.setC(Bytes.toFloat(b));
			}
			
			b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.ETF_COL);
			uf.setETF(Bytes.toFloat(b));
			
			b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.SR_COL);
			uf.setSR(Bytes.toBoolean(b));
			
			b = result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.SPAM_COL);
			if (b == null) {			// default for not not spam
				uf.setSpam(0);
			} else {						// spam
				uf.setSpam(1);
			}
			
			String userfeature = Float.toString(uf.getRD())+" "+Float.toString(uf.getETF())
					+" "+uf.isSR()+" "+uf.getC()+" "+uf.isSpam();

			context.write(new Text(userid), new Text(userfeature));

			// context.write(new Text(Bytes.toString(values)), new
			// Text(Bytes.toString(values2)));
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "ExportUserFeatures");
		job.setJarByClass(ExportUserFeatures.class);
		
		Scan scan2 = new Scan();
		scan2.setCaching(500);
		scan2.setCacheBlocks(false);
		scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
				UserFeatureTable.TAB_NAME);
		

		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(UserFeatureTable.TAB_NAME), // input table
				scan2,													// Scan instance to control CF and attribute selection
				ExportUserFeaturesMapper.class, 								// mapper class
				Text.class, 										// mapper output key
				Text.class, 								// mapper output value
				job);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setMapperClass(ExportUserFeaturesMapper.class);
//		
		
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, new Path("/home/brofan/桌面/UserFeature"));
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

	//	HadoopUtil.delete(conf, new Path(conf.get("temp.randomforest.path")));
		int res = ToolRunner.run(conf, new ExportUserFeatures(), args);
		System.exit(res);
	}
}
