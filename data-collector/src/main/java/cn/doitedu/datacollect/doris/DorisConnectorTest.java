package cn.doitedu.datacollect.doris;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DorisConnectorTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1,18,ZS
        DataStreamSource<String> ds = env.socketTextStream("localhost", 4444);
        SingleOutputStreamOperator<Stu> stuDs = ds.map(new MapFunction<String, Stu>() {
            @Override
            public Stu map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Stu(Integer.parseInt(arr[0]), Byte.parseByte(arr[1]), arr[2]);
            }
        });

        tenv.createTemporaryView("tmp",stuDs);

        /*tenv.executeSql("select * from tmp").print();*/

        // 创建doris连接器表
        tenv.executeSql(
                " CREATE TABLE flink_doris_sink (                  " +
                        "     id INT     ,                                 " +
                        "     age TINYINT,                                 " +
                        "     name STRING                                  " +
                        " )                                                " +
                        " WITH                                             " +
                        " (                                                " +
                        "       'connector' = 'doris',                     " +
                        "       'fenodes' = 'doitedu:8030',                " +
                        "       'table.identifier' = 'doit31.stu',         " +
                        "       'username' = 'root',                       " +
                        "       'password' = '',                           " +
                        "       'sink.label-prefix' = 'flink_doris_stu'    " +
                        " )                                                "
        );


        // 从socket数据表中，select数据  ，insert到doris连接器表
        tenv.executeSql("insert into flink_doris_sink select id,age,name from tmp");



        env.execute();

    }
}
