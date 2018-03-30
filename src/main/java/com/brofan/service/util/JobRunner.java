package com.brofan.service.util;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class JobRunner implements Runnable {
	private JobControl control;
	
	public JobRunner(JobControl control) {
		this.control = control;
	}

	public void run() {
		this.control.run();
	}
}
