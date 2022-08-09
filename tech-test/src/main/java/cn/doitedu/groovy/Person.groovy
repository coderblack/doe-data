package cn.doitedu.groovy

class Person {
   def String sayName1(String name){
        return "hello " + name
    }

    def String sayName2(String name , String suffix){
        return name + " " + suffix
    }


    def int add(int a, int  b){
        return a+b
    }
}
