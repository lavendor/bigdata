package com.lavendor.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCJob {

    public static void  main(String[] args){
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("mapred.jar","E:\\myprojects\\bigdata\\target\\bigdata-1.0-SNAPSHOT.jar");

        try{
            //实例化job
            Job job = Job.getInstance(conf);

            //设置程序入口
            job.setMapperClass(WCMapper.class);
            //设置mapper输出Key的数据类型
            job.setMapOutputKeyClass(Text.class);
            //设置mapper输出value的数据类型
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce类
            job.setReducerClass(WCReducer.class);

            //读入文件路径
            FileInputFormat.addInputPath(job,new Path("/input/hello.txt"));
            //MapReduce之后结果路径
            Path output = new Path("/out/wordcount/");
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(output)){
                fs.delete(output,true);
            }
            FileOutputFormat.setOutputPath(job,new Path("/out/wordcount/"));

            //等待job任务执行完毕
            boolean flag = job.waitForCompletion(true);
            if(flag){
                System.out.println("job success!");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
