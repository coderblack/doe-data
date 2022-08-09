package cn.doitedu;

import cn.doitedu.groovy.Person;

public class GroovyHello {
    public static void main(String[] args) {
        Person person = new Person();
        System.out.println(person.sayName1("taoge"));
        System.out.println(person.sayName2("涛哥", " 深似海男人"));
        System.out.println(person.add(3, 5));

    }
}