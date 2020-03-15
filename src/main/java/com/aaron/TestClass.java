package com.aaron;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestClass {
    public static void main(String[] args){
        String times = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println(times);
    }

}
