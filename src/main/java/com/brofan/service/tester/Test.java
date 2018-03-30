package com.brofan.service.tester;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by wislish on 15/9/15.
 */
public class Test {



    public static void main(String[] args){

        int result1,num1=25,num2=40,num3=17,num4=5;
        double result2,v1=12.78;
        float ab = 5;
        result2=ab;
        System.out.println(result2);
        result2=num1/num4;
        System.out.println(result2);
        result1=num3/num4;
        System.out.println(result1);
        result1=(int)v1/num4;
        System.out.println(result1);
        result2=(double)num1/num2;
        System.out.println(result2);

        new Test().test();

    }

    public void test(){
        int a=0;
        final int MIN=0;


    }
}
