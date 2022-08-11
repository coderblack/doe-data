package cn.doitedu.rtdw.etl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MallAppTrafficReport1 {
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
                        " releaseChannel    STRING   "+
                        " ) WITH (                                                " +
                        "  'connector' = 'kafka',                                 " +
                        "  'topic' = 'dws-traffic-analyse',                       " +
                        "  'properties.bootstrap.servers' = 'doitedu:9092',       " +
                        "  'properties.group.id' = 'testGroup',                   " +
                        "  'format' = 'json',                                     " +
                        "  'scan.startup.mode' = 'earliest-offset'                " +
                        " )                                                       ");

        tenv.executeSql("select * from dws_traffic_analyse")/*.print()*/;




        // 创建输出到mysql的连接器表
        tenv.executeSql(
                " CREATE TABLE rt_traffic_rpt1 (                " +
                        "   dt   string,                                  " +
                        "   pv_amt  bigint,                               " +
                        "   uv_amt  bigint,                               " +
                        "   session_amt bigint,                           " +
                        "   max_session_timelong bigint,                  " +
                        "   min_session_timelong bigint,                  " +
                        "   avg_session_timelong bigint,                  " +
                        "   PRIMARY KEY (dt) NOT ENFORCED                 " +
                        " ) WITH (                                        " +
                        "    'connector' = 'jdbc',                        " +
                        "    'url' = 'jdbc:mysql://doitedu:3306/rtmk',    " +
                        "    'table-name' = 'rt_traffic_rpt1',            " +
                        "    'username' = 'root' ,                        " +
                        "    'password' = 'root' ,                        " +
                        "    'sink.parallelism' = '1' ,                   " +
                        "    'sink.buffer-flush.interval' = '1s',         " +
                        "    'sink.max-retries' = '3'                     " +
                        " )                                               "
        );

        // 报表统计，并输出实时统计结果 到  mysql

        tenv.executeSql(" INSERT INTO  rt_traffic_rpt1                 "+
                " SELECT                                                        "+
                "   dt,                                                         "+
                "   sum(pv) as pv_amt,                                          "+
                "   count(distinct guid) as uv_amt,                             "+
                "   count(1) as session_amt,                                    "+
                "   max(session_timelong) as max_session_timelong,              "+
                "   min(session_timelong) as min_session_timelong,              "+
                "   avg(session_timelong) as avg_session_timelong               "+
                " FROM                                                          "+
                " (                                                             "+
                "    SELECT                                                     "+
                "      date_format(to_timestamp_ltz(ts,3),'yyyy-MM-dd') AS dt,  "+
                "      splitSessionId as session_id,                            "+
                "      guid,                                                    "+
                "      count(if(eventId='pageLoad',1,cast('' as int))) as pv,   "+
                "      max(ts) - min(ts) as session_timelong                    "+
                "    from  dws_traffic_analyse                                  "+
                "    group by                                                   "+
                "      date_format(to_timestamp_ltz(ts,3),'yyyy-MM-dd'),        "+
                "      splitSessionId,guid                                      "+
                " )                                                             "+
                " GROUP BY dt                                                   "
        );
    }
}
