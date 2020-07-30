package com.lavendor.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 切分第一次mr结果
 */
public class FOFMapperTwo extends Mapper<LongWritable,Text,Friend,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //cat-hadoop-2
        String[] strs = StringUtils.split(value.toString(),'-');
        Friend f = new Friend();
        f.setFriend1(strs[0]);
        f.setFriend2(strs[1]);
        f.setHot(Integer.parseInt(strs[2]));
        context.write(f,new IntWritable(f.getHot()));

        //亲密度互相都要推送 前后换个位置
        Friend f2 = new Friend();
        f2.setFriend2(strs[0]);
        f2.setFriend1(strs[1]);
        f2.setHot(Integer.parseInt(strs[2]));
        context.write(f2,new IntWritable(f.getHot()));
    }
}
