package cn.doitedu.rtmk.engine.main;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.functions.Json2UserEventMapFunction;
import cn.doitedu.rtmk.engine.functions.Row2RuleMetaBeanMapFunction;
import cn.doitedu.rtmk.engine.functions.RuleMatchProcessFunction;
import cn.doitedu.rtmk.engine.functions.RuleMatchProcessFunctionOld;
import cn.doitedu.rtmk.engine.pojo.RuleMatchResult;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import cn.doitedu.rtmk.engine.utils.FlinkStateDescriptors;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Objects;

public class RuleEngine {

    public static void main(String[] args) throws Exception {
        // 创建编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");
        //env.setParallelism(2000);
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 接收用户行为事件
        // 从kafka读入商城用户行为日志
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("doe-rtmk")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("rtmk-events")
                .build();

        // {"guid":1,"eventId":"e1","properties":{"p1":"v1","p2":"2"},"eventTime":100000}
        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");
        SingleOutputStreamOperator<UserEvent> userEventStream = sourceStream.map(new Json2UserEventMapFunction());


        // 用cdc抓取规则系统的元数据库中的规则定义变更数据（新增规则，停用规则，删除规则）
        tenv.executeSql("CREATE TABLE rule_meta_cdc (    " +
                " `id` int   PRIMARY KEY NOT ENFORCED,    " +
                " `rule_id` string  ,                     " +
                " `rule_model_id` int   ,                 " +
                " `rule_profile_user_bitmap` binary  ,    " +
                " `caculator_groovy_code` string,         " +
                " `rule_param_json` string,               " +
                " `creator_name` string  ,                " +
                " `rule_status` int   ,                   " +
                " `create_time` timestamp(3)  ,           " +
                " `update_time` timestamp(3)              " +
                "  ) WITH (                                        " +
                "     'connector' = 'mysql-cdc',                   " +
                "     'hostname' = 'doitedu'   ,                   " +
                "     'port' = '3306'          ,                   " +
                "     'username' = 'root'      ,                   " +
                "     'password' = 'root'      ,                   " +
                "     'database-name' = 'rtmk',                    " +
                "     'table-name' = 'rule_instance_definition'    " +
                ")");

        //tenv.executeSql("select * from rule_meta_cdc").print();
        Table table = tenv.sqlQuery("select * from rule_meta_cdc");
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);

        DataStream<RuleMetaBean> ruleMetaBeanStream = rowDataStream.map(new Row2RuleMetaBeanMapFunction()).filter(Objects::nonNull);

        // 将规则元信息流广播出去
        BroadcastStream<RuleMetaBean> ruleMetaBeanBroadcastStream = ruleMetaBeanStream.broadcast(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor);


        // 连接用户行为事件流  和  规则变更数据流
        DataStream<JSONObject> resultStream = userEventStream
                .keyBy(UserEvent::getGuid)
                .connect(ruleMetaBeanBroadcastStream)
                // 处理用户事件，进行规则运行和匹配
                .process(new RuleMatchProcessFunction());

        // 打印规则匹配结果
        resultStream.print();

        env.execute();
    }


}
