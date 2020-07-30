package com.lavendor.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 天气执行job
 */
public class WeatherJob {

    public static void main(String[] args){
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.set("mapred.jar","E:\\myprojects\\bigdata\\target\\bigdata-1.0-SNAPSHOT.jar");
        try {
            Job job = Job.getInstance(conf);
            job.setJobName(WeatherJob.class.getName());
            job.setJarByClass(WeatherJob.class);
            //设置mapper类
            job.setMapperClass(WeatherMapper.class);
            job.setMapOutputKeyClass(Weather.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置reduce类
            job.setReducerClass(WeatherReducer.class);
            //指定分区、排序、分组、task个数
            job.setPartitionerClass(WeatherPartitioner.class);
            job.setSortComparatorClass(WeatherSort.class);
            job.setGroupingComparatorClass(WeatherGroup.class);
            job.setNumReduceTasks(3);//task数量
            //job读入文件路径
            FileInputFormat.addInputPath(job,new Path("/input/tc02.txt"));

            //写入HDFS
            Path out = new Path("/out/tc2/");
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(out)){
                fs.delete(out,true);
            }
//            fs.create(out);
            FileOutputFormat.setOutputPath(job,new Path("/out/tc2/"));

            //读HDFS中的结果文件并且打印
//            FSDataInputStream fin = fs.open(out);
//            System.out.println(fin.readUTF());

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
