package cn.doitedu.rtmk.tech_test.groovytest.java;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.sql.*;

public class DynamicCallGroovy {
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");


        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select groovy_code from t_dynamic_code");

        resultSet.next();
        String groovyCodeStr = resultSet.getString("groovy_code");

        //
        GroovyClassLoader classLoader = new GroovyClassLoader();

        // 解析源代码，编译成class
        Class groovyClass = classLoader.parseClass(groovyCodeStr);
        GroovyObject person = (GroovyObject) groovyClass.newInstance();

        String param1 = "taoge";
        String result1 = (String) person.invokeMethod("saySomeThing", param1);

        System.out.println("在java中代码中，打印 groovy代码调用后的返回值： " + result1);


    }
}
