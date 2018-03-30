package com.brofan.service.feature.review;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.brofan.table.TestDataTable;
import com.brofan.table.TestTable;
import org.ansj.domain.Term;
import org.ansj.recognition.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.table.ReviewDataTable;
import com.brofan.table.ReviewFeatureTable;
import com.brofan.table.entity.Nature;
import com.brofan.table.entity.ReviewFeature;

public class LenAndPartOfSpeech extends Configured implements Tool {

	public static class LenAndPartOfSpeechMapper
			extends TableMapper<ImmutableBytesWritable, Put> {

		private byte[] table;
		private byte[] family;
		private byte[] qualifier;

		public void setup(Context context) {

			Configuration conf = context.getConfiguration();
			String tname = conf.get("tname");
			if (tname.equals("TestTable")){
				table = TestDataTable.TAB_NAME;
				family = TestDataTable.FAM_NAME;
				qualifier=TestTable.LENGTH_COL;
			}else{
				table = ReviewFeatureTable.TAB_NAME;
				family = ReviewFeatureTable.FAM_NAME;
				qualifier=ReviewFeatureTable.LENGTH_COL;
			}
		}
		@Override
		public void map(ImmutableBytesWritable key, Result result, Context context)
				throws IOException, InterruptedException {
			byte[] rowkey = result.getRow();
			
			byte[] b = result.getValue(
					family,
					com.brofan.table.entity.Text.getDataCol("body")
			);
			String body = Bytes.toString(b);
			List<Term> terms = ToAnalysis.parse(body);
			new NatureRecognition(terms).recognition();
			HashMap<String, Double> statistics = new HashMap<String, Double>();
			
			double len = (double)terms.size();
			
			// get nature frequency
			for (Term term : terms) {
				String natureStr = term.getNatrue().natureStr;
				if (statistics.containsKey(natureStr)){
					statistics.put(natureStr, statistics.get(natureStr) + 1/len);
				} else {
					statistics.put(natureStr, 1/len);
				}
			}
			Put put = new Put(rowkey);
			// put nature frequency
			Nature.putData(put, family, statistics);
			// put review body length
			put.add(
				family,
				qualifier,
				Bytes.toBytes(body.length()));
			
			context.write(null, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Job job = new Job(conf, "LenAndPartOfSpeech");
		job.setJarByClass(LenAndPartOfSpeech.class);

		byte[] inputtable=null;
		byte[] outputtable=null;
		String tname = conf.get("tname");
		if (tname.equals("TestTable")){
		 inputtable = TestDataTable.TAB_NAME;
		 outputtable = TestTable.TAB_NAME;
		}else{
			inputtable = ReviewDataTable.TAB_NAME;
			outputtable = ReviewFeatureTable.TAB_NAME;
		}

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(inputtable),	// input table
				scan, 												// Scan instance to control CF and attribute selection
				LenAndPartOfSpeechMapper.class,				// mapper class
				ImmutableBytesWritable.class,					// mapper output key
				Put.class,											// mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(outputtable),
				null,
				job);
		job.setNumReduceTasks(0);
		return job;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = configureJob(conf);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(HBaseConfiguration.create(), new LenAndPartOfSpeech(), args);
		System.exit(res);
	}
}
