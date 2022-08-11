package cn.doitedu.rtdw.etl;

import cn.doitedu.rtdw.etl.functions.TrafficAnalyseFunc;
import cn.doitedu.rtdw.etl.pojo.EventBean;
import cn.doitedu.rtdw.etl.pojo.TrafficBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class MallAppTrafficDwsEtl {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 构造一个 kafka 的source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("dwd-applog-detail2")
                .setBootstrapServers("doitedu:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("tr")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

        // 从kafka的dwd明细topic读取数据
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "dwd-applog");

        // 带上事件时间语义和watermark生成策略的  bean对象数据流
        DataStream<EventBean> beanStream = sourceStream.map(json -> JSON.parseObject(json, EventBean.class));

        DataStream<TrafficBean> trafficStream = beanStream
                .keyBy(new KeySelector<EventBean, Tuple2<Long,String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(EventBean value) throws Exception {
                        return Tuple2.of(value.getGuid(), value.getSessionid());
                    }
                })
                .process(new TrafficAnalyseFunc());


        // 构造用于输出到kafka的sink算子
        KafkaSink<String> resultSink = KafkaSink.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic("dws-traffic-analyse")
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 将结果数据，转成json输出到kafka
        trafficStream.map(JSON::toJSONString)
                        .sinkTo(resultSink);

        env.execute();

    }

}
