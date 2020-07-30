package com.lavendor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.util.Date;

/**
 * Hello world!
 *
 */
public class HadoopTest {

    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Path root = new Path("hdfs://192.168.236.100:9000/");
        conf.set("fs.defaultFS","hdfs://192.168.236.100:9000");
        FileSystem fs = FileSystem.get(conf);

        //从本地文件上传到HDFS系统
//        fs.copyFromLocalFile(new Path("f:/user.json"),new Path("/user.json.copy"));
//        System.out.print(new Date());
//        fs.copyFromLocalFile(new Path("D:/downloads/iso/CentOS-6.5-x86_64-bin-DVD2.iso"),new Path("/root/CentOS-6.5-x86_64-bin-DVD2.iso"));
//        System.out.print(new Date());

        //列出根目录下的文件及文件夹
        FileStatus[] fileStatus = fs.listStatus(root);
        for(FileStatus file : fileStatus){
            System.out.println(file.getPath().getName());
        }

        //创建HDFS文件 并且读取
        Path newFile = new Path("hdfs://192.168.236.100:9000/newfile.txt");
        if(fs.exists(newFile)){
            fs.delete(newFile,false);
        }
        //写文件到HDFS
        FSDataOutputStream  out =  fs.create(newFile);
        out.writeUTF("hello world !");
        out.close();

        //从HDFS读文件
        FSDataInputStream input = fs.open(newFile);
        String str = input.readUTF();
        System.out.println(str);
        input.close();

        fs.close();
    }
}
