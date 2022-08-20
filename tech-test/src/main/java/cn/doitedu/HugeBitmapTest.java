package cn.doitedu;

import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class HugeBitmapTest {

    public static void main(String[] args) throws Exception {
        // 存10亿个用户id到 bitmap

        RoaringBitmap bm = RoaringBitmap.bitmapOf();

        for(int i=0;i<100000000;i++){
            bm.add(i);
        }

        // 12513208
        System.out.println(bm.serializedSizeInBytes());

        FileOutputStream fout = new FileOutputStream(new File("d:/bitmap.dat"));
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        bm.serialize(dout);


        Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        PreparedStatement stmt = connection.prepareStatement("insert into bm_test (id,bm) values(?,?)");
        stmt.setInt(1,1);
        stmt.setBytes(2,bout.toByteArray());
        stmt.execute();

        dout.close();
        bout.close();
        fout.close();

    }
}
