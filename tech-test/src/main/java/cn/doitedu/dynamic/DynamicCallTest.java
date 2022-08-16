package cn.doitedu.dynamic;

import java.sql.*;

public class DynamicCallTest {


    public static void main(String[] args) throws InterruptedException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

        System.out.println("我在工作中.....");

        System.out.println("我在工作中.....");


        System.out.println("我要调一个工具来做加法");


        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        Statement stmt = conn.createStatement();


        while(true) {

            ResultSet rs = stmt.executeQuery("select class_name,java_code from t_dynamic_code");
            while(rs.next()) {
                String class_name = rs.getString("class_name");
                String java_code = rs.getString("java_code");

                // 利用编译工具，对源代码进行编译

                // 编译完成后，加载class

                // 反射调用
                Class<?> aClass = Class.forName(class_name);  // 加载class（编译好的东西） ，不是  java源代码
                Calculator calculator = (Calculator) aClass.newInstance();
                int res = calculator.add(10, 20);

                System.out.println("调用工具完毕，得到了结果：" + res);
                Thread.sleep(2000);
            }
        }

    }


}
