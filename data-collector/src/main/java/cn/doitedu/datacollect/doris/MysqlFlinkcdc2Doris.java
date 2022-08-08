package cn.doitedu.datacollect.doris;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/8
 * @Desc: 从mysql同步变更数据到doris的测试程序
 *
 * -- doris中的目标表：
 *     create table stu_score(
 *       id int not null comment "学员id"
 *       ,name string
 *       ,gender string
 *       ,score float
 *     )
 *     unique key(id)
 *     distributed by hash(id) buckets 2
 *     properties(
 *       "replication_num"="1"
 *     );
 *
 *
 *
 **/
public class MysqlFlinkcdc2Doris {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 创建一个mysql的cdc连接器表
        tenv.executeSql("CREATE TABLE flink_mysql_cdc_stuscore (    " +
                "      id INT,                                 " +
                "      name STRING,                            " +
                "      gender   STRING,                        " +
                "      score FLOAT,                            " +
                "     PRIMARY KEY (id) NOT ENFORCED            " +
                "     ) WITH (                                 " +
                "     'connector' = 'mysql-cdc',      " +
                "     'hostname' = 'doitedu'   ,      " +
                "     'port' = '3306'          ,      " +
                "     'username' = 'root'      ,      " +
                "     'password' = 'root'      ,      " +
                "     'database-name' = 'flinktest',  " +
                "     'table-name' = 'flink_score'    " +
                ")");

        /*tenv.executeSql("select * from flink_mysql_cdc_stuscore").print();*/


        // 创建一个 doris的连接器表
        tenv.executeSql(
                " CREATE TABLE flink_doris_sink_stuscore (        " +
                        "      id INT,                                     " +
                        "      name STRING,                                " +
                        "      gender   STRING,                            " +
                        "      score FLOAT,                                " +
                        "     PRIMARY KEY (id) NOT ENFORCED                " +
                        " )                                                " +
                        " WITH                                             " +
                        " (                                                " +
                        "       'connector' = 'doris',                     " +
                        "       'fenodes' = 'doitedu:8030',                " +
                        "       'table.identifier' = 'doit31.stu_score',   " +
                        "       'username' = 'root',                       " +
                        "       'password' = '',                           " +
                        "       'sink.label-prefix' = 'flink_stu_score'    " +
                        " )                                                "
        );

        // insert into .. select ..
        tenv.executeSql("insert into flink_doris_sink_stuscore select * from flink_mysql_cdc_stuscore");




    }
}
