package com.lavendor.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 上传文件到HDFS
 */
public class UploadToHDFS {

    public static void main(String[] args) throws Exception{
        Path filePath = new Path("hdfs://master:9000/input/fof.txt");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        Path localPath = new Path("E:\\myprojects\\hdfs_files\\fof.txt");

        //避免重复上传
        if(fs.exists(filePath)){
            fs.delete(filePath,true);
        }
        fs.copyFromLocalFile(localPath,filePath);
        fs.close();

        System.out.println("文件上传成功.");
    }

}
