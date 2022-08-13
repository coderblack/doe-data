package cn.doitedu.utils;

import cn.doitedu.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.roaringbitmap.RoaringBitmap;

public class Utils {


    public static DataStream<Event> getEventDataStream(DataStreamSource<String> ds) {
        DataStream<Event> eventDs = ds.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] split = value.split(",");
                return new Event(Integer.parseInt(split[0]), split[1]);
            }
        });

        return eventDs;
    }



    public static DataStream<RoaringBitmap> getKafkaBitmap( StreamExecutionEnvironment env){

        // 构造一个 kafka 的source
        KafkaSource<RoaringBitmap> kafkaSource = KafkaSource.<RoaringBitmap>builder()
                .setTopics("bm-test")
                .setBootstrapServers("doitedu:9092")
                .setValueOnlyDeserializer(new BitmapSchema())
                .setGroupId("tr")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        // 人群画像bitmap topic获取数据
        DataStreamSource<RoaringBitmap> ds = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"bm");

        return ds;
    }

    public static DataStream<String> getKafkaBitmapBase64(StreamExecutionEnvironment env) {
        // 构造一个 kafka 的source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("bm-test-base64")
                .setBootstrapServers("doitedu:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("tr")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        // 人群画像bitmap topic获取数据
        DataStreamSource<String> ds = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"bmbase64");

        return ds;
    }
}
