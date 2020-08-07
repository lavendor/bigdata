package com.lavendor.hadoop;

import javax.xml.soap.Text;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {

    public static void main(String[] args){

//        Test.hashCodeTest();


        try{
            Test.dateFormat();
            System.out.println("this is normal");
            throw new Exception("exception");
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("this is exec");
        }

        System.out.println("after exception");
        int a = 0;
        a = 10;
        System.out.println(a);

    }

    //测试hashCode值
    public static void hashCodeTest(){
        String s = "hello";
//        int n = 23;
        System.out.println(s.hashCode() & Integer.MAX_VALUE);
        System.out.println("world".hashCode());
    }

    //时间格式化
    public static void dateFormat() throws Exception{
        String line = "1952-12-02 14:21:01|34c";
        String dateStr = line.split("\\|")[0];
        System.out.println(dateStr);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(dateStr));
        System.out.println(cal.get(Calendar.YEAR));
        System.out.println(cal.get(Calendar.MONTH) + 1);
        System.out.println(cal.get(Calendar.DAY_OF_MONTH));
        System.out.println(cal.get(Calendar.HOUR_OF_DAY));
        System.out.println(cal.get(Calendar.MINUTE));
        System.out.println(cal.get(Calendar.SECOND));
        throw new Exception("this is dateFormat exception");
    }

}
