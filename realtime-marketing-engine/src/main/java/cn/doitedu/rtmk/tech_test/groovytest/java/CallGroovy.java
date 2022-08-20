package cn.doitedu.rtmk.tech_test.groovytest.java;

import cn.doitedu.rtmk.tech_test.groovytest.groovy.Caculator;

public class CallGroovy {

    public static void main(String[] args) {

        Caculator groovyCaculator = new Caculator();

        int res = groovyCaculator.add(20, 30);

        System.out.println("调用groovy类add方法的结果： "  +  res);

    }

}
