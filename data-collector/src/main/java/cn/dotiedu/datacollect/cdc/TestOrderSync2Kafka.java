package cn.dotiedu.datacollect.cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/7/31
 * @Desc: 业务库 ，订单表 ，同步到kafka
 *
 * 测试环境准备：  mysql中要创建一个测试的订单表：
 *     CREATE TABLE `test_order` (
 *        `id` int(11) NOT NULL AUTO_INCREMENT,
 *        `memeber_id` int(11) DEFAULT NULL,
 *        `pay_type` int(11) DEFAULT NULL,
 *        `order_amount` double DEFAULT NULL,
 *        `pay_amount` double DEFAULT NULL,
 *        `order_status` varchar(255) DEFAULT NULL,
 *        `create_time` datetime DEFAULT NULL,
 *        `modify_time` datetime DEFAULT NULL,
 *        PRIMARY KEY (`id`)
 *      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * kafka中要创建一个 topic ：  test
 * [root@doitedu]#  kafka-topics --zookeeper doitedu:2181 --create --topic test-order --parititions 1 --replication-factor 1
 *
 **/
public class TestOrderSync2Kafka {

    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**
         `id` int(11) NOT NULL AUTO_INCREMENT,
         `memeber_id` int(11) DEFAULT NULL,
         `pay_type` int(11) DEFAULT NULL,
         `order_amount` double DEFAULT NULL,
         `pay_amount` double DEFAULT NULL,
         `order_status` varchar(255) DEFAULT NULL,
         `create_time` datetime DEFAULT NULL,
         `modify_time` datetime DEFAULT NULL,
         */
        // 建cdc连接器源表
        tableEnv.executeSql("CREATE TABLE flink_order (    " +
                "      id INT,                                      " +
                "      memeber_id INT,                               " +
                "      pay_type   INT,                               " +
                "      order_amount DOUBLE,                          " +
                "      pay_amount   DOUBLE,                          " +
                "      order_status STRING,                          " +
                "      create_time TIMESTAMP(0),                     " +
                "      modify_time TIMESTAMP(0),                     " +
                "      tbname  STRING  METADATA FROM 'table_name' ,  " +
                "     PRIMARY KEY (id) NOT ENFORCED                  " +
                "     ) WITH (                                       " +
                "     'connector' = 'mysql-cdc',      " +
                "     'hostname' = 'doitedu'   ,      " +
                "     'port' = '3306'          ,      " +
                "     'username' = 'root'      ,      " +
                "     'password' = 'root'      ,      " +
                "     'database-name' = 'flinktest',  " +
                "     'table-name' = 'test_order'    " +
                ")");

        // 从上面定义的表中，读取数据，本质上，就是通过表定义中的连接器，去抓取数据
        tableEnv.executeSql("select * from flink_order")/*.print()*/;


        // 创建一张  用kafka-connector定义的表
        tableEnv.executeSql(
                " CREATE TABLE kafka_order (                 " +
                        "      id INT,                                       " +
                        "      memeber_id INT,                               " +
                        "      pay_type   INT,                               " +
                        "      order_amount DOUBLE,                          " +
                        "      pay_amount   DOUBLE,                          " +
                        "      order_status STRING,                          " +
                        "      create_time TIMESTAMP(0),                     " +
                        "      modify_time TIMESTAMP(0),                     " +
                        "      modify_time_long BIGINT,                      " +
                        "      x_tbname STRING,                              " +  // 这个字段，将进入kafka的message的key中
                        "      x_id     int   ,                              " +  // 这个字段，将进入kafka的message的key中
                        "     primary key (x_id, x_tbname)  not enforced       " +
                        " ) WITH (                                                " +
                        "  'connector' = 'upsert-kafka',                          " +
                        "  'topic' = 'test-order',                                " +
                        "  'properties.bootstrap.servers' = 'doitedu:9092',       " +
                        "  'properties.group.id' = 'testGroup',                   " +
                        "  'key.format'='json',                                   " +
                        "  'key.json.ignore-parse-errors' = 'true',               " +
                        "  'key.fields-prefix'='x_',                              " +  // 需要放入kafka的message的key中的字段前缀
                        "  'value.format'='json',                                 " +
                        "  'value.json.fail-on-missing-field'='false',            " +
                        "  'value.fields-include' = 'EXCEPT_KEY'                  " +
                        " )                                                       ");

        tableEnv.executeSql( "insert into kafka_order " +
                "select id,memeber_id,pay_type,order_amount,pay_amount,order_status, create_time, modify_time,  unix_timestamp(cast(modify_time as string))*1000, tbname, id " +
                "from flink_order");


    }


}
