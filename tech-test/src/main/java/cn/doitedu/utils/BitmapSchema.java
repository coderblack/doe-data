package cn.doitedu.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/12
 * @Desc: 用户flink-kafka连接器的bitmap序列化器
 **/
public class BitmapSchema implements DeserializationSchema<RoaringBitmap>, SerializationSchema<RoaringBitmap> {
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public RoaringBitmap deserialize(byte[] message) throws IOException {
        ByteArrayInputStream bi = new ByteArrayInputStream(message);
        DataInputStream din = new DataInputStream(bi);
        RoaringBitmap bm = RoaringBitmap.bitmapOf();

        bm.deserialize(din);
        din.close();
        return bm;
    }

    @Override
    public boolean isEndOfStream(RoaringBitmap nextElement) {
        return false;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(RoaringBitmap bm) {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        byte[] bytes ;
        try {
            bm.serialize(dos);
            bytes = bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return bytes;
    }

    @Override
    public TypeInformation<RoaringBitmap> getProducedType() {
        return TypeInformation.of(RoaringBitmap.class);
    }
}
