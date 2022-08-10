package cn.doitedu.rtdw.etl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class MallAppEventsPreprocess {

    public static void main(String[] args) throws Exception {

        /**
         * flink编程环境准备
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");

        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);


        // 从kafka读入商城用户行为日志
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("doe-01")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("mall-events")
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");



        // json解析
        DataStream<EventBean> beanStream = sourceStream.map(new JsonToEventBeanMapFunction());


        // 过滤脏数据
        DataStream<EventBean> filtered = beanStream.filter(new EventsDataFilterFunction());


        // 按设备id进行 keyby

        KeyedStream<EventBean, String> keyByed = filtered.keyBy(EventBean::getDeviceid);


        // guid生成
        keyByed.process(new GuidGenerateFunction());




        // 地域维度信息集成


        // 输出到kafka


        // 输出到doris



        env.execute();

    }


}
