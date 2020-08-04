package com.lavendor.hadoop.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class WeatherMapper extends Mapper<LongWritable,Text,Weather,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context){
        try{
            String line = value.toString();
            String dateStr = line.split("\\|")[0];
            String tempStr = line.split("\\|")[1];
            int temp = Integer.parseInt(tempStr.substring(0,tempStr.lastIndexOf('c')));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();
            cal.setTime(sdf.parse(dateStr));
            Weather w = new Weather();
            w.setYear(cal.get(Calendar.YEAR));
            w.setMonth(cal.get(Calendar.MONTH)+1);
            w.setDay(cal.get(Calendar.DAY_OF_MONTH));
            w.setTemp(temp);
            System.out.println("-----------------温度对象打印------------------------------");
            System.out.println(w.getYear()+"-"+w.getMonth()+"-"+w.getDay()+" "+w.getTemp());

            context.write(w,new IntWritable(temp));

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
