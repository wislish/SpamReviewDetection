package com.brofan.service.classifier.dt;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.net.URI;


/**
 * Created by wislish on 7/9/15.
 */
public class TransToLocal {

    public static void main(String args[]){

        Configuration conf = new Configuration();
        conf.addResource("project-site.xml");

        try {
            FileSystem hdfsFileSystem = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
            FileSystem hdfsFileSystem2 = FileSystem.get(conf);

            String result = "";

            Path src = new Path(conf.get("temp.export.path"));
            Path dst = new Path(conf.get("temp.dtdata.path"));
            HadoopUtil.delete(conf, new Path(conf.get("temp.dtdata.path")));

            FileUtil.copyMerge(hdfsFileSystem, src, hdfsFileSystem, dst, false, conf, null);
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("error");
        }
//        String fileName = hdfs.getName();
//        hdfsFileSystem.copyToLocalFile(false, hdfs, local);
//        try {
//            FileSystem fs = FileSystem.get(conf);
//            FileStatus[] status = fs.listStatus(new Path(conf.get("temp.export.path")));
//            for(int i=0;i<status.length;i++){
//                System.out.println(status[i].getPath());
//               fs.copyToLocalFile(false, status[i].getPath(), new Path("localdir"));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}
