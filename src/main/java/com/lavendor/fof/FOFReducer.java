package com.lavendor.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer 过滤掉直接亲密度关系
 * reduce任务处理一个key,多个key循环获取值，聚合成一个
 */
public class FOFReducer extends Reducer<Text,IntWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        try{
            int sum = 0;
            boolean flag = true;
            for(IntWritable value : values){
                if(value.get() == 0){ //直接亲密度 不需要 在此个key中，只要有一个为0，则表示是直接亲密度，过滤不需要
                    flag = false;
                    break;
                }
                sum += value.get();
            }
            if(flag){
                // 输出间接亲密度关系
                context.write(new Text(key.toString() + "-" + sum),NullWritable.get());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
