package cn.doitedu.groovy

import groovy.util.logging.Slf4j

@Slf4j
class Person {
    def String sayName1(String name) {
        log.error("hahahaha")
        return "hello " + name
    }

    def String sayName2(String name, String suffix) {
        return name + " " + suffix
    }


    def int add(int a, int b) {
        return a + b
    }
}
