package cn.doitedu.rtmk.tech_test.bitmap_inject;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;

public class _03_FlinkCdcBitmapAndCall {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint/");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建cdc连接器表，去读mysql中的规则定义表的binlog
        tenv.executeSql("CREATE TABLE rtmk_rule_define (   " +
                "      rule_id STRING  PRIMARY KEY NOT ENFORCED,    " +
                "      profile_users_bitmap BINARY                  " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = 'doitedu'   ,         " +
                "     'port' = '3306'          ,         " +
                "     'username' = 'root'      ,         " +
                "     'password' = 'root'      ,         " +
                "     'database-name' = 'rtmk',          " +
                "     'table-name' = 'rtmk_rule_def'     " +
                ")");

        Table table = tenv.sqlQuery("select rule_id,profile_users_bitmap from rtmk_rule_define");

        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);

        rowDataStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                RowKind kind = row.getKind();
                if(kind == RowKind.INSERT){
                    String rule_id = row.<String>getFieldAs("rule_id");
                    byte[] bitmapBytes = row.<byte[]>getFieldAs("profile_users_bitmap");

                    // 反序列化本次拿到的规则的bitmap
                    RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
                    bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));

                    // 判断201,101,77 用户是否在其中
                    boolean res201 = bitmap.contains(201);
                    boolean res101 = bitmap.contains(101);
                    boolean res77 = bitmap.contains(77);

                    out.collect(String.format("规则：%s, 用户：201, 存在于规则人群否: %s ",rule_id,res201));
                    out.collect(String.format("规则：%s, 用户：101, 存在于规则人群否: %s ",rule_id,res101));
                    out.collect(String.format("规则：%s, 用户：77 , 存在于规则人群否: %s ",rule_id,res77));
                }

            }
        }).print();


        env.execute();

    }

}
