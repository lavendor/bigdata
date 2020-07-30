package com.lavendor.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FOFReducerTwo extends Reducer<Friend,IntWritable,Text,NullWritable> {

    @Override
    protected void reduce(Friend key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for(IntWritable i : values){
            String msg = key.getFriend1() + "-" + key.getFriend2() +": " + key.getHot();
            context.write(new Text(msg),NullWritable.get());
        }
    }
}
