package com.brofan.service.feature.user;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.brofan.service.feature.user.util.AvgWritable;
import com.brofan.table.ReviewDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.UserFeatureTable;
import com.brofan.table.entity.Score;

public class RD extends Configured implements Tool {
	public static class MapperClass 
		extends TableMapper<Text, AvgWritable> {
		
		private Hashtable<String, Float> shopAvgStar = new Hashtable<String, Float>();
		
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf =  context.getConfiguration();
			byte[] meanStarCol = Score.getMeanCol("star");
			
			HTable sFtrTable = new HTable(conf, ShopFeatureTable.TAB_NAME);
			Scan scan = new Scan();
			
			// never forget to set cache!!! T_T
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			
			scan.addColumn(ShopFeatureTable.FAM_NAME, meanStarCol);
			ResultScanner rs = sFtrTable.getScanner(scan);
			try {
				for (Result r : rs) {
					String key = Bytes.toString(r.getRow());
					byte[] b = r.getValue(ShopFeatureTable.FAM_NAME, meanStarCol);
					float value = Bytes.toFloat(b);
					
					shopAvgStar.put(key, value);
				}
			} finally {
				rs.close();
			}
			sFtrTable.close();
			
		}
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result result, Context context)
				throws IOException, InterruptedException {
			
			byte[] b = result.getValue(ReviewDataTable.FAM_NAME, Score.getDataCol("star"));
			int star = Bytes.toInt(b);
			
			String[] rDataKey = Bytes.toString(result.getRow()).split("_");
			String userid = rDataKey[0];
			String shopid = rDataKey[1];
			
			float diff = (float)Math.abs((float)star - shopAvgStar.get(shopid));
			 
			context.write(new Text(userid), new AvgWritable(1, diff));
		}
	}
	
	public static class CombinerClass
		extends Reducer<Text, AvgWritable, Text, AvgWritable> {
	
		@Override
		public void reduce(Text key, Iterable<AvgWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<AvgWritable> i = values.iterator();
			int num = 0;
			float sum = 0;
			while(i.hasNext()) {
				AvgWritable aw = i.next();
				num += aw.getNum().get();
				sum += aw.getSum().get();
			}
			
			context.write(key, new AvgWritable(num, sum));
		}
	}
	
	public static class ReducerClass
		extends TableReducer<Text, AvgWritable, ImmutableBytesWritable> {
		
		@Override
		public void reduce(Text key, Iterable<AvgWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<AvgWritable> i = values.iterator();
			int num = 0;
			float sum = 0;
			while(i.hasNext()) {
				AvgWritable aw = i.next();
				num += aw.getNum().get();
				sum += aw.getSum().get();
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(
					UserFeatureTable.FAM_NAME,
					UserFeatureTable.RD_COL,
					Bytes.toBytes(sum/num/4) );
			context.write(null, put);
		}
	}
	
	public static Job configureJob(Configuration conf) throws Exception {
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.addColumn(ReviewDataTable.FAM_NAME, Score.getDataCol("star"));
		
		Job job = new Job(conf, "RD");
		job.setJarByClass(RD.class);
		
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(ReviewDataTable.TAB_NAME),	// table name
				scan, 												// Scan instance to control CF and attribute selection
				MapperClass.class,								// mapper class
				Text.class,											// mapper output key
				AvgWritable.class,								// mapper output value
				job);
		job.setCombinerClass(CombinerClass.class);
		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(UserFeatureTable.TAB_NAME),
				ReducerClass.class,
				job); 
		return job;
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = configureJob(conf);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new RD(), args);
		System.exit(res);
	}
}