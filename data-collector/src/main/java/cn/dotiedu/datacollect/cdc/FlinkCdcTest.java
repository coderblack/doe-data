package cn.dotiedu.datacollect.cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/7/31
 * @Desc: flink-cdc 捕获 mysql变更数据测试代码
 **/
public class FlinkCdcTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建cdc连接器源表
        tableEnv.executeSql("CREATE TABLE flink_score (    " +
                "      id INT,                                      " +
                "      name string,                                 " +
                "      gender string,                               " +
                "      score double,                                " +
                "      tname string metadata from 'table_name',     " +
                "      dbname string metadata from 'database_name', " +
                "     PRIMARY KEY (id) NOT ENFORCED                 " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',      " +
                "     'hostname' = 'doitedu'   ,      " +
                "     'port' = '3306'          ,      " +
                "     'username' = 'root'      ,      " +
                "     'password' = 'root'      ,      " +
                "     'database-name' = 'flinktest',  " +
                "     'table-name' = 'flink_score'    " +
                ")");

        // 从上面定义的表中，读取数据，本质上，就是通过表定义中的连接器，去抓取数据
        tableEnv.executeSql("select * from flink_score")/*.print()*/;


        // 实时报表统计：  查询每种性别中，成绩最高的前2个同学
        tableEnv.executeSql(
                " select                                                              "+
                        "   id,name,gender,score                                               "+
                        " from                                                                 "+
                        " (                                                                    "+
                        " select                                                               "+
                        "    id,                                                               "+
                        "    name,                                                             "+
                        "    gender,                                                           "+
                        "    score,                                                            "+
                        "    row_number() over(partition by gender order by score desc) as rn  "+
                        " from flink_score ) o                                                 "+
                        " where rn<=2                                                          "
        ).print();


    }

}
