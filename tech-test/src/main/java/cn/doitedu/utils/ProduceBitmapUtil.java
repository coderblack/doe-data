package cn.doitedu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.roaringbitmap.RoaringBitmap;

import java.util.Properties;

public class ProduceBitmapUtil {

    public static void main(String[] args) {

        Properties props = new Properties();
        //设置kafka集群的地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "doitedu:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定自定义的roaringbitmap序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaBitmapSerializer.class.getName());


        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("retries", 3);
        props.put("batch.size", 1);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,204800);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 102400000);  // 默认32M

        // 构造一个生产者实例对象
        KafkaProducer<String, RoaringBitmap> producer = new KafkaProducer<>(props);


        // 构造一个测试用的bitmap
        RoaringBitmap bm = RoaringBitmap.bitmapOf();
        //bm.add(1,3,4,5,6,7,8,11);
        bm.add(1,6,7);

        // 构造producer消息对象
        ProducerRecord<String, RoaringBitmap> record = new ProducerRecord<>("bm-test", bm);
        // 发送消息
        producer.send(record);

        // 关闭生产者
        producer.flush();
        producer.close();

    }


}
