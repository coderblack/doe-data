package cn.doitedu.rtdw.etl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdShowClickPatternRecognize {

    public static void main(String[] args) {

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
        Configuration conf = new Configuration();
        conf.setInteger("table.exec.resource.default-parallelism",1);
        tEnv.getConfig().addConfiguration(conf);


        /**
         * 创建 连接 kafka中的 广告曝光点击事件日志的连接器表
         * {"guid":1,"ad_id":"ad01","event_id":"adShow","event_time":1000,"tracking_id":"tracking01"}
         */
        tEnv.executeSql(
                "CREATE TABLE kafka_ad_event_source (        " +
                        "    guid   BIGINT,                           "+
                        "    ad_id  STRING,                           "+
                        "    event_id STRING,                         "+
                        "    event_time  BIGINT,                      "+
                        "    tracking_id STRING,                      "+
                        "    rt AS to_timestamp_ltz(event_time,3),    "+
                        "    WATERMARK FOR  rt  AS    rt - INTERVAL '0' SECOND   "+
                        ") WITH (                                        " +
                        "  'connector' = 'kafka',                        " +
                        "  'topic' = 'dwd-ad-events',                    " +
                        "  'properties.bootstrap.servers' = 'doitedu:9092',     " +
                        "  'properties.group.id' = 'ad-g0',            " +
                        "  'format' = 'json'  ,                        " +
                        "  'scan.startup.mode' = 'latest-offset'       " +
                        ")");


        tEnv.executeSql("select * from kafka_ad_event_source")/*.print()*/;


        tEnv.executeSql(
                " SELECT                                                                   "+
                        "    guid,                                                                 "+
                        "    ad_id,                                                                "+
                        "    show_time,                                                            "+
                        "    click_time                                                            "+
                        " FROM kafka_ad_event_source                                               "+
                        "   MATCH_RECOGNIZE(                                                       "+
                        "   PARTITION BY guid                                                      "+
                        "   ORDER BY rt                                                            "+
                        "   MEASURES                                                               "+
                        "     A.guid AS a_guid,                                                      "+
                        "     A.ad_id AS ad_id,                                                    "+
                        "     A.event_time AS show_time,                                           "+
                        "     B.event_time as click_time                                           "+
                        "   ONE ROW PER MATCH                                                      "+
                        "   AFTER MATCH SKIP TO NEXT ROW                                           "+
                        "   PATTERN(A C* B)                                                        "+
                        "   DEFINE                                                                 "+
                        "      A  AS   A.event_id = 'adShow'  ,                                    "+
                        "      B  AS   B.event_id = 'adClick' AND  B.tracking_id = A.tracking_id , "+
                        "      C  AS   NOT C.tracking_id = A.tracking_id                           "+
                        "                                                                          "+
                        "   ) AS t                                                                 "
        ).print();


    }

}
