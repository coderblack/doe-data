package cn.doitedu.rtmk.tech_test.groovytest.java;

import groovy.lang.GroovyClassLoader;

import java.sql.*;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/16
 * @Desc: groovy代码动态调用示例2
 **/
public class DynamicCallGroovy2 {
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select groovy_code from t_dynamic_code");

        while(resultSet.next()) {
            String groovyCodeStr = resultSet.getString("groovy_code");
            GroovyClassLoader classLoader = new GroovyClassLoader();
            // 解析源代码，编译成class
            Class groovyClass = classLoader.parseClass(groovyCodeStr);

            // 对加载好的class，反射对象
            Person person = (Person) groovyClass.newInstance();

            // 调用对象方法
            String result = person.saySomeThing("涛哥");

            System.out.println("在java中代码中，打印 groovy代码调用后的返回值： " + result);
        }

        resultSet.close();
        conn.close();

    }
}
