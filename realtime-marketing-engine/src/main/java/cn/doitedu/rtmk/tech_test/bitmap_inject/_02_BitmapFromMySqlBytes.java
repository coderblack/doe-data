package cn.doitedu.rtmk.tech_test.bitmap_inject;

import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.sql.*;

public class _02_BitmapFromMySqlBytes {

    public static void main(String[] args) throws Exception {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        // 先从mysql中获得  g01_rule01
        PreparedStatement statement = conn.prepareStatement("select rule_id,profile_users_bitmap from rtmk_rule_def where rule_id = ? ");
        statement.setString(1,"g01_rule01");

        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        byte[] bitmapBytes = resultSet.getBytes("profile_users_bitmap");

        statement.close();
        conn.close();



        // 反序列化bitmap字节数组
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));


        // 测试，反序列化出来bitmap是否是之前序列化的数据
        // 序列化之前的bitmap中包含的guid：   1,3,5,7,101,201
        System.out.println(bitmap.contains(201));
        System.out.println(bitmap.contains(101));
        System.out.println(bitmap.contains(7));
        System.out.println(bitmap.contains(77));
        System.out.println(bitmap.contains(88));


    }

}
