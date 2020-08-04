package com.lavendor.hadoop.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Weather,IntWritable,Text,NullWritable> {

    @Override
    protected void reduce(Weather weather, Iterable<IntWritable> iterable, Context context){
        try{
            int f = 0;
            for(IntWritable value : iterable){
                f++;
                if(f>2){
                    break;
                }
                String result = weather.getYear() + "-" + weather.getMonth() + "-" + weather.getDay() + " " + value.get();
                System.out.println("-----------------reduce结果打印---------------------------");
                System.out.println(result);
                context.write(new Text(result),NullWritable.get());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
