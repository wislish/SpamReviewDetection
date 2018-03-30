package com.brofan.service.classifier.dt;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

public class RandomForestClassify {

	private String dataPath;

	private String datasetPath;

	private String modelPath;

	private String resultPath;

	public void testForest() throws Exception {

		Configuration conf = new Configuration();
		conf.addResource("project-site.xml");


		dataPath = conf.get("temp.dtdata.path");

		datasetPath = conf.get("temp.dtinfo.path");
		modelPath = conf.get("temp.dtoutput.path");
		resultPath = conf.get("temp.dtpredict.path");

		System.out.println(" "+dataPath+" "+datasetPath+" "+modelPath+" "+resultPath);
		String[] testArgs = new String[] {
				"--input", dataPath,
				"--dataset",datasetPath,
				"--model", modelPath, 
				"--output",resultPath, 
				"--mapreduce", 
				"--analyze"

		};

		HadoopUtil.delete(new Configuration(),new Path(testArgs[Arrays.asList(testArgs).indexOf("--output") + 1]));
		TestForest.main(testArgs);
	}

	public static void main(String args[]) throws Exception {
		RandomForestClassify forest = new RandomForestClassify();
		forest.testForest();
	}
}
