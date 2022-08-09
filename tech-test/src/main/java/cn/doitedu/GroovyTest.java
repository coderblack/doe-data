package cn.doitedu;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class GroovyTest {
    public static void main(String[] args) throws Exception {

        String s = FileUtils.readFileToString(new File("tech-test/src/main/java/cn/doitedu/groovy/Person.groovy"));

        GroovyClassLoader classLoader = new GroovyClassLoader();

        Class groovyClass = classLoader.parseClass(s);
        GroovyObject groovyObject = (GroovyObject) groovyClass.newInstance();

        String param1 = "taoge";
        String[] param2 = {"taoge", "总裁"};
        Integer[] param3 = {8, 7};

        Object result1 = groovyObject.invokeMethod("sayName1", param1);
        Object result2 = groovyObject.invokeMethod("sayName2", param2);
        Object result3 = groovyObject.invokeMethod("add", param3);
        System.out.println(result1);
        System.out.println(result2);
        System.out.println(result3);

    }
}
