package com.brofan.service.feature.shop;

import com.brofan.table.ShopDataTable;
import com.brofan.table.ShopFeatureTable;
import com.brofan.table.entity.Score;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by wislish on 15-6-25.
 */
public class Mean extends Configured implements Tool {

    public static class MeanStdDevMapper extends
            TableMapper<Text, LongWritable> {

        private LongWritable rate = new LongWritable();
        private Text shopId = new Text();
        private String[] scoreNames;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            scoreNames = conf.getStrings("job.MeanStdDev.scoreNames");
        }

        public void map(ImmutableBytesWritable row, Result result, Context context)
                throws IOException, InterruptedException {

            for (int i = 0; i< scoreNames.length; i++) {
                try {
                    byte[] b = result.getValue(ShopDataTable.FAM_NAME, Score.getDataCol(scoreNames[i]));
                    rate.set((long) (Bytes.toInt(b)));

                } catch (IllegalArgumentException ie) {

                    return;
                }

                String[] sDataKey = Bytes.toString(result.getRow()).split("_");

                // Build OutKey String
                StringBuffer outKey = new StringBuffer(sDataKey[0]);
                outKey.append("_");
                outKey.append(scoreNames[i]);
//                if (sDataKey[0].equals("11556824")) {
//                    System.out.println("key: " +outKey.toString()+" value:"+ rate.get());
//                }
                shopId.set(outKey.toString());
                context.write(shopId, rate);
            }
        }
    }

    public static class MeanStdDevReducer extends
            TableReducer<Text, LongWritable, Put> {

//        private int keyNum=0;

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {


            Iterator<LongWritable> value = values.iterator();
            long v =0;
            float num=0;
            double sum = 0;

            while (value.hasNext()){
                long thisvalue = value.next().get();
                v+=thisvalue;
                num++;
                sum=sum+Math.pow((double)thisvalue,2);
            }

            float mean = v/num;
            if (mean<-1 || mean >5){
                System.out.println("issue!!!!!!!!!!");
            }
            double sd = Math.sqrt((sum/num)-mean*mean);
            System.out.println("Mean: "+mean+" sd "+ sd+" V "+v+" num "+num);

            String[] inKey = key.toString().split("_");
            String shopId = inKey[0];
            String scoreName = inKey[1];
//            if (shopId.equals("11556824")) {
//                System.out.println("key: " +shopId+" Mean: "+mean+" V "+v+" num "+num);
//            }
//            System.out.println("Key NUm: "+keyNum);
            Put put = ShopFeatureTable.getPut(shopId);
            ShopFeatureTable.putMean(put, scoreName, mean);
            ShopFeatureTable.putSD(put, scoreName, (float) sd);
            context.write(null, put);
        }

    }





    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setStrings("job.MeanStdDev.scoreNames", Score.getAllScore());

        Job job = configureJob(conf);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

        return 0;
    }

    public static Job configureJob(Configuration conf) throws Exception {
        Job job = new Job(conf, "MeanStdDev");
        job.setJarByClass(Mean.class);

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(
                Bytes.toString(ShopDataTable.TAB_NAME),    // input table
                scan,                                                // Scan instance to control CF and attribute selection
                MeanStdDevMapper.class,                            // mapper class
                Text.class,                                            // mapper output key
                LongWritable.class,                        // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                Bytes.toString(ShopFeatureTable.TAB_NAME),// output table
                MeanStdDevReducer.class,						// reducer class
                job);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Mean(), args);
        System.exit(res);
    }
}
