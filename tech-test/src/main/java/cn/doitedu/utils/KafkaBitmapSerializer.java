package cn.doitedu.utils;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class KafkaBitmapSerializer implements Serializer<RoaringBitmap> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, RoaringBitmap bm) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        byte[] bytes = new byte[0];
        if(bm != null) {
            try {
                bm.serialize(dos);
                bytes = bos.toByteArray();
            } catch (IOException e) {

                throw new RuntimeException(e);
            }
        }
        return bytes;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, RoaringBitmap data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
