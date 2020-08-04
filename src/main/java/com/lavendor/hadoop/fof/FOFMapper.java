package com.lavendor.hadoop.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 切分数据
 */
public class FOFMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String[] people = StringUtils.split(value.toString(),' ');
            for(int i = 0;i<people.length;i++){
                //直接亲密度 标记为0
                String result = compare(people[0],people[i]);
                context.write(new Text(result),new IntWritable(0));
                for(int j = i+1;j<people.length;j++){
                    //二度亲密度 标记为1
                    String v = compare(people[i],people[j]);
                    context.write(new Text(v),new IntWritable(1));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 去除重复 tom-cat cat-tom被认定为同一数据
     * @param p1
     * @param p2
     * @return
     */
    public String compare(String p1,String p2){
        int r = p1.compareTo(p2);
        if(r<0){
            return p1 + "-" + p2;
        }
        return p2 + '-' + p1;
    }
}
