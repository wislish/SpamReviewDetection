package com.brofan.service.classifier.dt;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.data.DescriptorException;
import org.apache.mahout.classifier.df.tools.Describe;
import org.apache.mahout.common.HadoopUtil;

public class RandomForest {

	private String dataPath;

	private String infoPath;
	private String datasetPath;

   private String outputPath;

	private String m = "5"; // Number of variables to select at each tree-node

//	private String modelPath;
//
//	private boolean complemented; // tree is complemented
//
//	private Integer minSplitNum; // minimum number for split
//
//	private Double minVarianceProportion; // minimum proportion of the total
											// variance for split
	private String nbTrees = "200"; // Number of trees to grow

//	private Long seed; // Random seed
//
//	private String isPartial; // use partial data implementation
//
//	private String useMapreduce;
	
	public  void describeDate() throws IOException,DescriptorException {

		Configuration conf = new Configuration();
		conf.addResource("project-site.xml");


		dataPath = conf.get("temp.dtdata.path");
//		infoPath = "hdfs://master:9000/review/temp/Export/part-m-00000";

		datasetPath = conf.get("temp.dtinfo.path");

		String[] describeArgs = new String[]{"-p",dataPath,"-f",datasetPath,"-d","15","N","L"};

		HadoopUtil.delete(new Configuration(), new Path(describeArgs[Arrays.asList(describeArgs).indexOf("-f")+1]));

      	Describe.main(describeArgs);
	}
	
	public void buildForest() throws Exception{

		Configuration conf = new Configuration();
		conf.addResource("project-site.xml");

		outputPath = conf.get("temp.dtoutput.path");

		String[] buildArgs = new String[]{
				"--data",dataPath
				,"--dataset",datasetPath
				,"--output",outputPath
				,"--nbtrees",nbTrees
				,"--partial"
				,"--selection",m
				
		};
		
		HadoopUtil.delete(new Configuration(), new Path(buildArgs[Arrays.asList(buildArgs).indexOf("--output")+1]));
		BuildForest.main(buildArgs);
	}
	
	public static void main(String args[]) throws Exception {


		RandomForest forest = new RandomForest();
		forest.describeDate();
		forest.buildForest();
	
	}
	
	
}
