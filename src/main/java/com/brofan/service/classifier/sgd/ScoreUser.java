//package com.brofan.service.classifier.sgd;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.mahout.classifier.sgd.CrossFoldLearner;
//import org.apache.mahout.classifier.sgd.ModelSerializer;
//import org.apache.mahout.math.SequentialAccessSparseVector;
//import org.apache.mahout.math.Vector;
//
//import com.brofan.service.classifier.sgd.entity.UserEncoder;
//import com.brofan.service.classifier.sgd.entity.UserFeatures;
//import com.brofan.table.UserFeatureTable;
//
//public final class ScoreUser extends Configured implements Tool {
//	
//	public static class ScoreUserMapper
//			extends TableMapper<ImmutableBytesWritable, Put> {
//		
//		private CrossFoldLearner learner;
//		private UserEncoder encoder = new UserEncoder();
//		
//		@Override
//		public void setup(Context context)
//				throws IOException, InterruptedException {
//			try {
//				String path = context.getConfiguration().get("model.user.path");
//				InputStream in = new FileInputStream(path);
//				learner = ModelSerializer.readBinary(in, CrossFoldLearner.class);
//				in.close();
//			} catch (FileNotFoundException fe) {
//				fe.printStackTrace();
//				context.setStatus("Model File Not Found!");
//			}
//		}
//
//		@Override
//		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
//				throws IOException, InterruptedException {
//			
//			UserFeatures uf = new UserFeatures();
//			uf.setRD(Bytes.toFloat(result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.RD_COL)));
//			//uf.setC(Bytes.toFloat(result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.C_COL)));
//			uf.setETF(Bytes.toFloat(result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.ETF_COL)));
//			uf.setSR(Bytes.toBoolean(result.getValue(UserFeatureTable.FAM_NAME, UserFeatureTable.SR_COL)));
//			
//			Vector vector = new SequentialAccessSparseVector(10);
//			encoder.addToVector(uf, vector);
//			double score = learner.classifyScalar(vector);
//			System.out.println("Score: " + score);
//			
//			Put put = new Put(result.getRow());
//			put.add(
//				UserFeatureTable.FAM_NAME,
//				UserFeatureTable.SCORE_COL,
//				Bytes.toBytes(score)
//			);
//			context.write(null, put);
//		}
//	}
//	
//	public static Job configureJob(Configuration conf) throws Exception {
//		Scan scan = new Scan();
//		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
//		scan.setCacheBlocks(false);  // don't set to true for MR jobs
//		scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.RD_COL);
////		scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.C_COL);
//		scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.SR_COL);
//		scan.addColumn(UserFeatureTable.FAM_NAME, UserFeatureTable.ETF_COL);
//		
//		Job job = new Job(conf, "ScoreUser");
//		job.setJarByClass(ScoreUser.class);
//		
//		TableMapReduceUtil.initTableMapperJob(
//				Bytes.toString(UserFeatureTable.TAB_NAME),// table name
//				scan, 												// Scan instance to control CF and attribute selection
//				ScoreUserMapper.class,							// mapper class
//				ImmutableBytesWritable.class,					// mapper output key
//				Put.class,											// mapper output value
//				job);
//		TableMapReduceUtil.initTableReducerJob(
//				Bytes.toString(UserFeatureTable.TAB_NAME),
//				null,
//				job);
//		job.setNumReduceTasks(0);
//		
//		return job;
//	}
//
//	public int run(String[] arg0) throws Exception {
//		Configuration conf = getConf();
//
//		Job job = configureJob(conf);
//		
//		boolean b = job.waitForCompletion(true);
//		if (!b) {
//			throw new IOException("error with job!");
//		}
//		
//		return 0;
//	}
//	
//	public static void main(String[] args) throws Exception {
//		Configuration conf = HBaseConfiguration.create();
//		conf.addResource("project-site.xml");
//		
//		int res = ToolRunner.run(conf, new ScoreUser(), args);
//		System.exit(res);
//	}
//}
package com.brofan.service.classifier.sgd;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.CrossFoldLearner;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
//import org.apache.mahout.math.stats.OnlineSummarizer;

import com.brofan.service.classifier.sgd.entity.UserEncoder;
import com.brofan.service.classifier.sgd.entity.UserFeatures;

public class ScoreUser extends Configured {
	
	private UserHelper helper = new UserHelper();
	private boolean showScores = true;
	
	public void score() throws IOException {
		String path = getConf().get("model.user.path");
		InputStream in = new FileInputStream(path);
		CrossFoldLearner learner = ModelSerializer.readBinary(in, CrossFoldLearner.class);
		if (learner == null) {
			// not trained
		}
		in.close();

		Auc collector = new Auc();
		UserEncoder encoder = new UserEncoder();
		
		if (showScores) {
			System.out.println("Category\tThe score for category 1\tLogLikelihood");
		}
		
		int k = 0;

		while (helper.hasNext()) {
			Vector v = new SequentialAccessSparseVector(helper.getFeaturesCount());
			UserFeatures uf = helper.getUserFeatures();
			encoder.addToVector(uf, v);
			
			double score =  learner.classifyScalar(v);

			k++;
			 
			if (k % 1000 == 0 && showScores) {
				System.out.printf("%d\t%.3f\t%.6f%n",
						uf.isSpam(), score, learner.logLikelihood(uf.isSpam(), v));
			}
			collector.add(uf.isSpam(), score);
		}

		System.out.printf("AUC = %.2f%n", collector.auc());

		Matrix m = collector.confusion();
		System.out.printf("confusion: [[%.1f, %.1f], [%.1f, %.1f]]%n",
				m.get(0, 0), m.get(1, 0), m.get(0, 1), m.get(1, 1));
		m = collector.entropy();
		System.out.printf("entropy: [[%.1f, %.1f], [%.1f, %.1f]]%n",
				m.get(0, 0), m.get(1, 0), m.get(0, 1), m.get(1, 1));

	}
	
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		conf.addResource("project-site.xml");
		
		ScoreUser su = new ScoreUser();
		su.setConf(conf);
		su.score();
		
	}
}