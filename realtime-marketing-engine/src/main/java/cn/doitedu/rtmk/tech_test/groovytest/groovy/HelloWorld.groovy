package cn.doitedu.rtmk.tech_test.groovytest.groovy

class HelloWorld {

    static void main(String[] args) {

        println("hello groovy")

        // 调用另外一个groovy类
        def caculator = new Caculator()
        int res = caculator.add(10, 20)

        println("工具调用的结果为： " + res)

    }
}
