package cn.doitedu.rtmk.tech_test.bitmap_inject;


import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.*;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/16
 * @Desc:  模拟规则发布平台，并测试其人群bitmap生成及发布功能
 **/
public class _01_RulePublisher {

    public static void main(String[] args) throws IOException, SQLException {

        //String ruleId = "g01_rule01";
        //String ruleId = "g01_rule02";
        String ruleId = "g01_rule03";

        // 根据规则的条件，去es中查询人群
        //int[] ruleProfileUsers = {1,3,5,7,101,201};
        //int[] ruleProfileUsers = {11,3,5,7,301,202,666};
        int[] ruleProfileUsers = {55,3,5,7};

        // 把查询出来的人群的guid列表，变成bitmap
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(ruleProfileUsers);

        // 把生成好的bitmap，序列到一个字节数组中
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        bitmap.serialize(dout);
        byte[] bitmapBytes = bout.toByteArray();

        // 将这个bitmap连同本规则的其他信息，一同发布到规则平台的元数据库中
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        PreparedStatement statement = conn.prepareStatement("insert into rtmk_rule_def values(?,?)");
        statement.setString(1,ruleId);
        statement.setBytes(2,bitmapBytes);

        statement.execute();

        statement.close();
        conn.close();

    }
}
