package com.brofan.service.trainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import com.brofan.service.classifier.dt.ExportReviewData;
import com.brofan.service.classifier.dt.RandomForest;
import com.brofan.service.classifier.dt.TransToLocal;
import com.brofan.service.classifier.sgd.TrainShopModel;
import com.brofan.service.classifier.sgd.TrainUserModel;
import com.brofan.service.feature.shop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.mahout.common.HadoopUtil;


import com.brofan.service.feature.review.HasSpam;
//import com.brofan.service.classifier.dt.RandomForest;
//import com.brofan.service.classifier.sgd.TrainShopModel;
//import com.brofan.service.classifier.sgd.TrainUserModelMahout;
import com.brofan.service.feature.review.LenAndPartOfSpeech;
import com.brofan.service.feature.review.Scored;
import com.brofan.service.feature.review.Stard;
import com.brofan.service.feature.user.C;
import com.brofan.service.feature.user.ETF;
import com.brofan.service.feature.user.LastReviewTime;
import com.brofan.service.feature.user.RD;
import com.brofan.service.feature.user.SR;
import com.brofan.service.util.JobRunner;
import com.brofan.table.ReviewFeatureTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.TableManager;
import com.brofan.table.UserFeatureTable;
import com.brofan.table.entity.Score;

public class Trainer {
	public static void main(String[] args) throws Exception {
		new Trainer().start();
	}
	
	public void setTable(Configuration conf) throws IOException {
		TableManager.setup(conf, UserFeatureTable.TAB_NAME, UserFeatureTable.FAM_NAME);
		TableManager.setup(conf, ShopFeatureTable.TAB_NAME, ShopFeatureTable.FAM_NAME);
		TableManager.setup(conf, ReviewFeatureTable.TAB_NAME, ReviewFeatureTable.FAM_NAME);
	}
	
	public void start() {
		try {
			System.out.println("Start Training Program...");
			Configuration conf = HBaseConfiguration.create();
			conf.addResource("project-site.xml");
			conf.set("tname", "TrainingTable");
			setTable(conf);
			
			System.out.println("Submiting MapReduce Jobs...");
			startAllJobs(conf);
			System.out.println("MapReduce Jobs Finished");



//			System.out.println("Training User Model...");
//			TrainUserModel uModel = new TrainUserModel();
//			uModel.setConf(conf);
//			uModel.trainWithLibLinear();
//
//			System.out.println("Training Shop Model...");
//			TrainShopModel sModel = new TrainShopModel();
//			sModel.setConf(conf);
//			sModel.trainWithLibLinear();
//
//			HadoopUtil.delete(conf, new Path(conf.get("temp.export.path")));
//			Job exportJob = ExportReviewData.configureJob(conf);
//			boolean b = exportJob.waitForCompletion(true);
//			if (!b) {
//				throw new IOException("error with export job!");
//			}
//
//			HadoopUtil.delete(conf, new Path(conf.get("temp.dtdata.path")));
//			TransToLocal.main(null);
//
////
//			System.out.println("Training Review Model...");
//			RandomForest forest = new RandomForest();
//			forest.describeDate();
//			forest.buildForest();
//
//			System.out.println("The training process is finished !!");
//
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void startAllJobs(Configuration conf) throws Exception {
		
		ArrayList<ControlledJob> jobList = new ArrayList<ControlledJob>(20);
		
		/************************ Setup Jobs ****************************/
		// Setup MeanStdDev Job
		conf.setStrings("job.MeanStdDev.scoreNames", Score.getAllScore());
		Job job = Mean.configureJob(conf);
		ControlledJob MeanStdDevJob = new ControlledJob(
				job.getConfiguration()
		);
		MeanStdDevJob.setJob(job);
		jobList.add(MeanStdDevJob);
		
		// Setup RD Job
		job = RD.configureJob(conf);
		ControlledJob RDJob = new ControlledJob(
				RD.configureJob(conf).getConfiguration()
		);
		RDJob.setJob(job);
		RDJob.addDependingJob(MeanStdDevJob);
		jobList.add(RDJob);
		
		// Setup RCV Job
		job = RCV.configureJob(conf);
		ControlledJob RCVJob = new ControlledJob(
				RCV.configureJob(conf).getConfiguration()
		);
		RCVJob.setJob(job);
		RCVJob.addDependingJob(MeanStdDevJob);
		jobList.add(RCVJob);
		
		// Setup SR Job
		job = SR.configureJob(conf);
		ControlledJob SRJob = new ControlledJob(
				job.getConfiguration()
		);
		SRJob.setJob(job);
		jobList.add(SRJob);
		
		// Setup SRr Job
		job = SRr.configureJob(conf);
		ControlledJob SRrJob = new ControlledJob(
				job.getConfiguration()
		);
		SRrJob.setJob(job);
		SRrJob.addDependingJob(SRJob);
		jobList.add(SRrJob);
		
		// Setup C Job
		// CCalJob
		HadoopUtil.delete(conf, new Path(conf.get("temp.c.path")));
		job = C.configureCalJob(conf);
		ControlledJob CCalJob = new ControlledJob(
				job.getConfiguration()
		);
		CCalJob.setJob(job);
		jobList.add(CCalJob);
		// CSumJob
		job = C.configureSumJob(conf);
		ControlledJob CSumJob = new ControlledJob(
				job.getConfiguration()
		);
		CSumJob.setJob(job);
		CSumJob.addDependingJob(CCalJob);
		jobList.add(CSumJob);
		
		// Setup ETF Job
		// FirstReviewTime
		HadoopUtil.delete(conf, new Path(conf.get("temp.etf.path")));
		job = FirstReviewTime.configureJob(conf);
		ControlledJob shopFirstRJob = new ControlledJob(
				job.getConfiguration()
		);
		shopFirstRJob.setJob(job);
		jobList.add(shopFirstRJob);
		// LastReviewTime
		job = LastReviewTime.configureJob(conf);
		ControlledJob userShopLastRJob = new ControlledJob(
				job.getConfiguration()
		);
		userShopLastRJob.setJob(job);
		jobList.add(userShopLastRJob);
		// ETF
		job = ETF.configureJob(conf);
		ControlledJob ETFJob = new ControlledJob(
				job.getConfiguration()
		);
		ETFJob.setJob(job);
		ETFJob.addDependingJob(shopFirstRJob);
		ETFJob.addDependingJob(userShopLastRJob);
		jobList.add(ETFJob);
		
		/****************** Setup Review Feature Jobs *******************/
		job = LenAndPartOfSpeech.configureJob(conf);
		ControlledJob LenAndPartOfSpeechJob = new ControlledJob(
				job.getConfiguration()
		);
		LenAndPartOfSpeechJob.setJob(job);
		jobList.add(LenAndPartOfSpeechJob);
		
		job = Scored.configureJob(conf);
		ControlledJob ScoredJob = new ControlledJob(
				job.getConfiguration()
		);
		ScoredJob.setJob(job);
		jobList.add(ScoredJob);
		
		job = Stard.configureJob(conf);
		ControlledJob StardJob = new ControlledJob(
				job.getConfiguration()
		);
		StardJob.setJob(job);
		StardJob.addDependingJob(MeanStdDevJob);
		jobList.add(StardJob);
		
		job = HasSpam.configureJob(conf);
		ControlledJob HasSpamJob = new ControlledJob(
				job.getConfiguration()
		);
		HasSpamJob.setJob(job);
		jobList.add(HasSpamJob);

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