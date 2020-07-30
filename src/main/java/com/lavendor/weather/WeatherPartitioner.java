package com.lavendor.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 设置partition个数
 */
public class WeatherPartitioner extends HashPartitioner<Weather,IntWritable> {

    @Override
    public int getPartition(Weather weather, IntWritable value, int numReduceTasks) {
        return (weather.getYear()-1949) % numReduceTasks;
//        return super.getPartition(weather, value, numReduceTasks);
    }
}
