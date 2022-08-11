package cn.doitedu.rtdw.etl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/11
 * @Desc:  - 最近10分钟的pv流量，uv量，会话数， 每1分钟更新一次
 **/
public class MallAppTrafficReport3 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建连接器表，来读取流量分析的dws层中间表
        tenv.executeSql(
                " CREATE TABLE dws_traffic_analyse (    " +
                        " guid              BIGINT,  "+
                        " sessionId         STRING,  "+
                        " splitSessionId    STRING,  "+
                        " eventId           STRING,  "+
                        " ts                BIGINT,  "+
                        " pageId            STRING,  "+
                        " pageLoadTime      BIGINT,  "+
                        " province          STRING,  "+
                        " city              STRING,  "+
                        " region            STRING,  "+
                        " deviceType        STRING,  "+
                        " isNew             INT   ,  "+
                        " releaseChannel    STRING,   "+
                        " rt AS to_timestamp_ltz(ts,3) , "+
                        " WATERMARK FOR rt AS rt - INTERVAL '0' SECONDS  "+
                        " ) WITH (                                                " +
                        "  'connector' = 'kafka',                                 " +
                        "  'topic' = 'dws-traffic-analyse',                       " +
                        "  'properties.bootstrap.servers' = 'doitedu:9092',       " +
                        "  'properties.group.id' = '0002',                        " +
                        "  'format' = 'json',                                     " +
                        "  'scan.startup.mode' = 'earliest-offset'                " +
                        " )                                                       ");

        /*tenv.executeSql("desc dws_traffic_analyse").print();*/


        // 创建输出到mysql的连接器表
        tenv.executeSql(
                " CREATE TABLE rt_traffic_rpt3 (                   " +
                        "   start_time   STRING,                            " +
                        "   end_time  STRING,                               " +
                        "   page_id   STRING,                               " +
                        "   pv_amt  bigint,                                 " +
                        "   uv_amt bigint,                                  " +
                        "   PRIMARY KEY (start_time,end_time,page_id) NOT ENFORCED  " +
                        " ) WITH (                                          " +
                        "    'connector' = 'jdbc',                          " +
                        "    'url' = 'jdbc:mysql://doitedu:3306/rtmk',      " +
                        "    'table-name' = 'rt_traffic_rpt3',              " +
                        "    'username' = 'root' ,                          " +
                        "    'password' = 'root' ,                          " +
                        "    'sink.parallelism' = '1' ,                     " +
                        "    'sink.buffer-flush.interval' = '1s',           " +
                        "    'sink.max-retries' = '3'                       " +
                        " )                                                 "
        );


        // 统计指标，并插入目标表
        tenv.executeSql(
                " INSERT INTO    rt_traffic_rpt3                                              "+
                " with tmp as                                                                          "+
                " (                                                                                    "+
                "   SELECT                                                                             "+
                "     *                                                                                "+
                "   FROM dws_traffic_analyse                                                           "+
                "   WHERE pageId in ('page006','page007','page008')                                    "+
                " )                                                                                    "+
                " SELECT                                                                               "+
                "   date_format(window_start,'yyyy-MM-dd HH:mm:ss'),                                   "+
                "   date_format(window_end,'yyyy-MM-dd HH:mm:ss'),                                     "+
                "   pageId,                                                                            "+
                "   count(if(eventId='pageLoad',1,cast('' as int)))  as pv_amt,                        "+
                "   count(distinct guid) as uv_amt                                                     "+
                " FROM TABLE(                                                                          "+
                "   CUMULATE(TABLE tmp, DESCRIPTOR(rt), INTERVAL '10' MINUTES, INTERVAL '24' HOURS)    "+
                " )                                                                                    "+
                " GROUP BY window_start, window_end,pageId                                             "
        ).print();



    }
}
