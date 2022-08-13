package cn.doitedu.rtdw.etl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MallOrderBrandTopnHour {

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
         * 创建 连接 kafka中的 订单宽表的 连接器表
         */
        tEnv.executeSql(
                "CREATE TABLE kafka_oms_order_wide_source (  " +
                        "    oid  INT,                            "+
                        "    pid  INT,                            "+
                        "    price FLOAT,                         "+
                        "    quantity  INT,                       "+
                        "    create_time timestamp(3),            "+
                        "    brand  STRING      ,                 "+
                        "    WATERMARK FOR create_time AS   create_time - INTERVAL '0' SECOND   "+
                        ") WITH (                                         " +
                        "  'connector' = 'kafka',                         " +
                        "  'topic' = 'dws-oms-order-wide',                " +
                        "  'properties.bootstrap.servers' = 'doitedu:9092',     " +
                        "  'properties.group.id' = 'gxxx',            " +
                        "  'format' = 'json'  ,                                 " +
                        "  'scan.startup.mode' = 'earliest-offset'              " +
                        ")");

        tEnv.executeSql("select * from kafka_oms_order_wide_source")/*.print()*/;


        /**
         * mysql结果报表的jdbc连接器表
         */
        tEnv.executeSql(
                " CREATE TABLE brand_order_topn_hour (                                "+
                        "   window_start timestamp,                                           "+
                        "   window_end timestamp,                                             "+
                        "   `rank` int,                                                       "+
                        "   brand STRING,                                                     "+
                        "   amt FLOAT,                                                        "+
                        "   PRIMARY KEY (window_start,window_end,brand,`rank`) NOT ENFORCED   "+
                        " ) WITH (                                                            "+
                        "    'connector' = 'jdbc',                                            "+
                        "    'url' = 'jdbc:mysql://doitedu:3306/rtmk',                        "+
                        "    'table-name' = 'brand_order_topn_hour',                          "+
                        "    'username' = 'root',                                             "+
                        "    'password' = 'root'                                              "+
                        " )                                                                   "
        );



        /**
         * 对维度打款后的订单数据，进行报表统计： 每小时的订单额最大的前10个品牌及其总交易额
         */
        tEnv.executeSql("create temporary view ov as select * from kafka_oms_order_wide_source where brand is not null");


        tEnv.executeSql(
                " INSERT INTO  brand_order_topn_hour                                                   " +
                " SELECT                                                                                        " +
                        "    window_start,window_end,cast(rn as int),brand,amt                                                  " +
                        " FROM                                                                                  " +
                        " (                                                                                     " +
                        "     SELECT                                                                            " +
                        "        window_start,                                                                  " +
                        "        window_end,                                                                    " +
                        "        brand,                                                                         " +
                        "        amt,                                                                           " +
                        "        ROW_NUMBER() OVER(PARTITION BY window_start,window_end ORDER BY amt DESC) AS rn  " +
                        "     FROM                                                                              " +
                        "     (                                                                                 " +
                        "         SELECT                                                                        " +
                        "           window_start,window_end,brand,sum(price * quantity) as amt                  " +
                        "         FROM  TABLE(                                                                  " +
                        "            TUMBLE(TABLE ov ,DESCRIPTOR(create_time), INTERVAL '1' HOURS)             " +
                        "         )                                                                             " +
                        "         GROUP BY window_start,window_end,brand                                        " +
                        "     )                                                                                 " +
                        " )                                                                                     " +
                        " WHERE rn<=2                                                                           "
        )/*.print()*/;



    }

}
