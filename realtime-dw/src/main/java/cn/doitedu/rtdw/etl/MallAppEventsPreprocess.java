package cn.doitedu.rtdw.etl;

import ch.hsr.geohash.GeoHash;
import cn.doitedu.rtdw.etl.functions.EventsDataFilterFunction;
import cn.doitedu.rtdw.etl.functions.GeoHashAreaQueryFunction;
import cn.doitedu.rtdw.etl.functions.GuidGenerateFunction;
import cn.doitedu.rtdw.etl.functions.JsonToEventBeanMapFunction;
import cn.doitedu.rtdw.etl.pojo.EventBean;
import cn.doitedu.rtdw.utils.SqlHolder;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/10
 * @Desc: 商城用户行为日志预处理
 **/
public class MallAppEventsPreprocess {

    public static void main(String[] args) throws Exception {

        /**
         * flink编程环境准备
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");
        env.setParallelism(1);


        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);




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
        DataStream<EventBean> guidedStream = keyByed.process(new GuidGenerateFunction());


        // 为了doris而做的一件事，把properties这个map类型字段，生成一个json字符串字段
        DataStream<EventBean> tmpStream = guidedStream.map(bean -> {
            Map<String, String> properties = bean.getProperties();
            String propsJson = JSON.toJSONString(properties);
            bean.setPropsJson(propsJson);
            return bean;
        });


        // 地域维度信息集成
        // 重新keyBy ,让subtask们查到的geohash码对应省市区结果信息，尽最大可能重用
        // 所以，需要让相同geohash码的数据，进入相同的 subtask
        // 所以，要按照geohash码进行keyby

        SingleOutputStreamOperator<EventBean> resultStream = tmpStream.map(bean -> {
                    double latitude = bean.getLatitude();
                    double longitude = bean.getLongitude();

                    String geohashCode = "";
                    try {
                        geohashCode = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 5);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    bean.setGeoHashCode(geohashCode);

                    return bean;
                }).keyBy(bean -> bean.getGeoHashCode())
                .process(new GeoHashAreaQueryFunction());

        /* *
         * 输出处理好主流的 行为明细日志数据
         * 使用的方式，是用flinksql的连接器表
         */
        // 注册成 flinksql的表（视图）
        tEnv.createTemporaryView("logdetail",resultStream, Schema.newBuilder()
                .columnByExpression("dw_date","date_format(from_unixtime(`timestamp`/1000),'yyyy-MM-dd')")  // 衍生字段
                .build());

        // 创建doris sink连接器表 : doris_appdetail_sink
        // doris中需要提前创建目标表： dwd.app_log_detail （在项目的sqls文件夹中有）
        tEnv.executeSql(SqlHolder.DORIS_DETAIL_SINK_DDL);

        // 创建kafka sink连接器表 : kafka_dwd_sink
        // kafka中需要提前创建目标topic :  dwd-applog-detail
        tEnv.executeSql(SqlHolder.KAFKA_DETAIL_SINK_DDL);


        // 执行sql ，插入doris 连接器表
        tEnv.executeSql(SqlHolder.DORIS_DETAIL_SINK_INSERT);
        // 执行sql ，插入kafka连接器表
        tEnv.executeSql(SqlHolder.KAFKA_DETAIL_SINK_DML);




        /* *
         * 输出测流中的解析失败的gps座标
         * 使用的方式，是用flinkcore的sink算子
         */
        // 构造一个用于接收 “未知gps坐标”的 kafka sink
        KafkaSink<String> unknownGpsSink = KafkaSink.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic("unknown-gps")
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        // 输出解析失败的gps座标
        DataStream<String> unknowGpsStream = resultStream.getSideOutput((new OutputTag<String>("unknown_gps", TypeInformation.of(String.class))));
        unknowGpsStream.sinkTo(unknownGpsSink);


        env.execute();

    }


}
