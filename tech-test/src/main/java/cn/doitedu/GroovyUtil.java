package cn.doitedu;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import groovy.lang.GroovyObject;

import java.io.File;

public class GroovyUtil {
    /**
     * 加载Groovy文件，返回GroovyObject对象
     */
    public static GroovyObject getGroovyObjectBy(File file) {
        GroovyObject groovyObject = null;
        try {
            GroovyClassLoader classLoader = new GroovyClassLoader(Thread.currentThread().getContextClassLoader());
            Class clazz = classLoader.parseClass(new GroovyCodeSource(file));
            groovyObject = (GroovyObject)clazz.newInstance();
        } catch (Exception e) {}
        return groovyObject;
    }


    public static GroovyObject getGroovyObjectByText(String text) {
        GroovyObject groovyObject = null;
        try {
            GroovyClassLoader classLoader = new GroovyClassLoader(Thread.currentThread().getContextClassLoader());
            Class clazz = classLoader.parseClass(text);
            groovyObject = (GroovyObject)clazz.newInstance();
        } catch (Exception e) {}
        return groovyObject;
    }


    public static GroovyObject getGroovyObject(String filePath) {
        return getGroovyObjectBy(new File(filePath));
    }

    /**
     * 执行GroovyObject对象的方法
     */
    static Object invokeMethod(GroovyObject groovyObject, String name, Object args) {
        return groovyObject.invokeMethod(name, args);
    }
}