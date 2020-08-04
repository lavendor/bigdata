package com.lavendor.hadoop.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FOFJob {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("mapred.jar","E:\\myprojects\\bigdata\\target\\bigdata-1.0-SNAPSHOT.jar");
        try {
            Job job = Job.getInstance(conf);
            job.setJobName(FOFJob.class.getName());
            job.setJarByClass(FOFJob.class);
            //设置mapper类
            job.setMapperClass(FOFMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置reduce类
            job.setReducerClass(FOFReducer.class);
            job.setNumReduceTasks(1);//task数量
            //job读入文件路径
            FileInputFormat.addInputPath(job,new Path("/input/fof.txt"));

            //写入HDFS
            Path out = new Path("/out/fof1/");
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(out)){
                fs.delete(out,true);
            }
            FileOutputFormat.setOutputPath(job,out);

            //等待job任务执行完毕
            boolean flag = job.waitForCompletion(true);
            if(flag){
                System.out.println("job success!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
