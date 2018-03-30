package com.brofan.service.tester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import com.brofan.service.classifier.dt.ExportReviewData;
import com.brofan.service.classifier.dt.RandomForest;
import com.brofan.service.classifier.dt.RandomForestClassify;
import com.brofan.service.classifier.dt.TransToLocal;
import com.brofan.table.TestDataTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import com.brofan.service.feature.review.LenAndPartOfSpeech;
import com.brofan.service.feature.review.Scored;
import com.brofan.service.feature.review.Stard;
import com.brofan.service.tester.dataloader.TestDataLoader;
import com.brofan.service.util.JobRunner;
import com.brofan.table.TableManager;
import com.brofan.table.TestTable;
import org.apache.mahout.common.HadoopUtil;
import org.jruby.RubyProcess;

public class Tester {
	public static void main(String[] args) throws Exception {
		new Tester().start();
	}
	
	public void start() {
		try {
			System.out.println("Start Testing Program...");
			Configuration conf = HBaseConfiguration.create();
			conf.addResource("project-site.xml");
			conf.set("tname", "TestTable");
			TableManager.setup(conf, TestTable.TAB_NAME, TestTable.FAM_NAME);
			TableManager.setup(conf, TestDataTable.TAB_NAME, TestDataTable.FAM_NAME);

			System.out.println("Submiting MapReduce Jobs...");
			startAllJobs(conf);
			System.out.println("MapReduce Jobs Finished");
			System.out.println("Export the testing review");

			HadoopUtil.delete(conf, new Path(conf.get("temp.export.path")));
			Job exportJob = ExportReviewData.configureJob(conf);
			boolean b = exportJob.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with export job!");
			}

			HadoopUtil.delete(conf, new Path(conf.get("temp.dtdata.path")));
			TransToLocal.main(null);

			System.out.println("Staring the random forest to test!");
			RandomForestClassify forest = new RandomForestClassify();
			forest.testForest();

			System.out.println("finished !");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
protected void startAllJobs(Configuration conf) throws Exception {
		
		ArrayList<ControlledJob> jobList = new ArrayList<ControlledJob>(5);
		
		/***************************** Setup Jobs *****************************/
		Job job = TestDataLoader.configureJob(conf);
		ControlledJob dataLoaderJob = new ControlledJob(
				job.getConfiguration()
		);
		dataLoaderJob.setJob(job);
		jobList.add(dataLoaderJob);

		job = LenAndPartOfSpeech.configureJob(conf);
		// TODO: Need Refactoring
		// reset job configuration( input & output table )
		Configuration jobConf = job.getConfiguration();
//		jobConf.set(TableInputFormat.INPUT_TABLE, Bytes.toString(TestTable.TAB_NAME));
//		jobConf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(TestTable.TAB_NAME));
		
		ControlledJob LenAndPartOfSpeechJob = new ControlledJob(jobConf);
		LenAndPartOfSpeechJob.setJob(job);
		LenAndPartOfSpeechJob.addDependingJob(dataLoaderJob);
		jobList.add(LenAndPartOfSpeechJob);
		
		job = Scored.configureJob(conf);
		jobConf = job.getConfiguration();
//		jobConf.set(TableInputFormat.INPUT_TABLE, Bytes.toString(TestTable.TAB_NAME));
//		jobConf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(TestTable.TAB_NAME));
		
		ControlledJob ScoredJob = new ControlledJob(jobConf);
		ScoredJob.setJob(job);
		ScoredJob.addDependingJob(dataLoaderJob);
		jobList.add(ScoredJob);
		
		job = Stard.configureJob(conf);
		jobConf = job.getConfiguration();
//		jobConf.set(TableInputFormat.INPUT_TABLE, Bytes.toString(TestTable.TAB_NAME));
//		jobConf.set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(TestTable.TAB_NAME));
//
		ControlledJob StardJob = new ControlledJob(jobConf);
		StardJob.setJob(job);
		StardJob.addDependingJob(dataLoaderJob);
//		StardJob.addDependingJob(MeanStdDevJob);
		jobList.add(StardJob);
		
		JobControl jc = new JobControl("review");
		jc.addJobCollection(jobList);
		
		JobRunner runner = new JobRunner(jc);
		Thread t = new Thread(runner);
		t.start();
		
		int i = 0;
		while(!jc.allFinished()) {
			if (i % 20 == 0) {
				System.out.println(new Date().toString() + "： Still running...");
				System.out.println("Running jobs: " + jc.getRunningJobList().toString());
				System.out.println("Waiting jobs: " + jc.getWaitingJobList().toString());
				System.out.println("Successful jobs: " + jc.getSuccessfulJobList().toString());
			}
			Thread.sleep(1000);
			i++;
		}
		
		if (jc.getFailedJobList() != null) {
			System.out.println("Failed jobs: " + jc.getFailedJobList().toString());
		}
	}
}
