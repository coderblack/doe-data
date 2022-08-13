package cn.doitedu.rtdw.etl;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MallOrderItemCdcdDwsEtl {

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


        // 创建业务库的oms_order_item表的cdc连接器表

        // 建cdc连接器源表
        // 关心的字段：  订单Id,商品id,单价,购买数量,订单创建时间
        tEnv.executeSql("CREATE TABLE oms_order_item_cdc_source (    " +
                "      oid INT,                                      " +
                "      pid INT,                                     " +
                "      price float,                                 " +
                "      quantity int,                                " +
                "      create_time timestamp(3),                    " +
                "      update_time timestamp(3),                    " +
                "      WATERMARK FOR create_time AS create_time - INTERVAL '0' SECOND,   " +
                "     PRIMARY KEY (oid,pid) NOT ENFORCED            " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = 'doitedu'   ,         " +
                "     'port' = '3306'          ,         " +
                "     'username' = 'root'      ,         " +
                "     'password' = 'root'      ,         " +
                "     'database-name' = 'rtmk',          " +
                "     'table-name' = 'oms_order_item'    " +
                ")");

        tEnv.executeSql("select * from oms_order_item_cdc_source")/*.print()*/;

        // 建cdc连接器源表
        // 关心的字段：  订单Id,商品id,单价,购买数量,订单创建时间
        tEnv.executeSql("CREATE TABLE pms_product_cdc_source (    " +
                "      pid INT,                                     " +
                "      brand String,                                " +
                "      create_time timestamp(3),                    " +
                "      update_time timestamp(3),                    " +
                "     PRIMARY KEY (pid) NOT ENFORCED                " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = 'doitedu'   ,         " +
                "     'port' = '3306'          ,         " +
                "     'username' = 'root'      ,         " +
                "     'password' = 'root'      ,         " +
                "     'database-name' = 'rtmk',          " +
                "     'table-name' = 'pms_product'       " +
                ")");

        tEnv.executeSql("select * from pms_product_cdc_source")/*.print()*/;
        /*tEnv.executeSql("CREATE TEMPORARY VIEW ov  AS" +
                "    select                                                                             " +
                "       o.oid,                                                                          " +
                "       o.pid,                                                                          " +
                "       o.price,                                                                        " +
                "       o.quantity,                                                                     " +
                "       o.create_time,                                                                  " +
                "       p.brand                                                                         " +
                "    from oms_order_item_cdc_source o                                                   " +
                "    left join pms_product_cdc_source p                                                 " +
                "    on o.pid=p.pid                                                                     " )*//*.print()*//*;*/
        //tEnv.executeSql("desc oms_order_item_cdc_source").print();
        // tEnv.executeSql("desc ov").print();


        /**
         * 创建一个维度打宽后的kafka连接器表
         */
        tEnv.executeSql(
                "CREATE TABLE kafka_oms_order_wide_sink (  " +
                        "    oid  INT,                            "+
                        "    pid  INT,                            "+
                        "    price FLOAT,                         "+
                        "    quantity  INT,                       "+
                        "    create_time timestamp(3),            "+
                        "    brand  STRING      ,                 "+
                        "    PRIMARY KEY (oid,pid)  NOT ENFORCED  "+
                ") WITH (                                         " +
                "  'connector' = 'upsert-kafka',                  " +
                "  'topic' = 'dws-oms-order-wide',                " +
                "  'properties.bootstrap.servers' = 'doitedu:9092',     " +
                "  'properties.group.id' = 'g_oms-order-wide',          " +
                "  'key.format' = 'json'  ,                            " +
                "  'value.format' = 'json'                              " +
                ")");

        /**
         * 对订单商品详情表 和  商品信息维度表  ，做JOIN打宽，并将结果插入到上面创建的kakfka中间表
         *
         */
        tEnv.executeSql(
                " INSERT INTO kafka_oms_order_wide_sink             "+
                        " select                             "+
                        "    o.oid,                          "+
                        "    o.pid,                          "+
                        "    o.price,                        "+
                        "    o.quantity,                     "+
                        "    o.create_time,                  "+
                        "    p.brand                         "+
                        " from oms_order_item_cdc_source o   "+
                        " left join pms_product_cdc_source p "+
                        " on o.pid=p.pid                     "
        );

        //tEnv.executeSql("select * from kafka_oms_order_wide_sink ").print();

    }


}
