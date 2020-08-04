package com.lavendor.hadoop.weather;

import com.google.gson.annotations.Since;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 天气对象
 */
public class Weather implements WritableComparable<Weather> {

    private int year;
    private int month;
    private int day;
    //温度
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }


    /**
     * 年月日做比较
     * @param weather
     * @return
     */
    public int compareTo(Weather weather) {
        int y = Integer.compare(this.year,weather.getYear());
        if(y == 0){
            int m = Integer.compare(this.month,weather.getMonth());
            if(m == 0){
                return Integer.compare(this.temp,weather.getTemp());
            }
            return m;
        }
        return y;
    }

    /**
     * 写方法
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.month);
        out.writeInt(this.day);
        out.writeInt(this.temp);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temp = in.readInt();
    }
}
