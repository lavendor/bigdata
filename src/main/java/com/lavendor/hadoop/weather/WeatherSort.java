package com.lavendor.hadoop.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSort extends WritableComparator {

    public WeatherSort() {
        super(Weather.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;

        int c1 = Integer.compare(w1.getYear(),w2.getYear());
        if(c1 == 0){
            int c2 = Integer.compare(w1.getMonth(),w2.getMonth());
            if(c2 == 0){
                return -Integer.compare(w1.getTemp(),w2.getTemp()); //温度降序排序
            }
            return c2;
        }
        return c1;

    }
}
