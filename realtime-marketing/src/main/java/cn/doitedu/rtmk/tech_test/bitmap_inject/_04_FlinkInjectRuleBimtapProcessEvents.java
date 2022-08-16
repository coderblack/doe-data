package cn.doitedu.rtmk.tech_test.bitmap_inject;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/16
 * @Desc: 从socket读取用户的实时的行为事件
 *        并从外部（规则管理平台的mysql源数据库）注入规则及规则对应的人群bitmap，对输入的行为事件进行规则处理
 **/

@Slf4j
public class _04_FlinkInjectRuleBimtapProcessEvents {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint/");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 获取用户实时行为事件流
        DataStreamSource<String> eventStr = env.socketTextStream("doitedu", 4444);
        SingleOutputStreamOperator<Tuple2<Integer, String>> events = eventStr.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String line) throws Exception {
                String[] split = line.split(",");
                return Tuple2.of(Integer.parseInt(split[0]), split[1]);
            }
        });


        // 获取规则系统的规则定义流
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
        DataStream<Tuple2<Tuple2<String,String>, RoaringBitmap >> ruleDefineStream = rowDataStream.map(new MapFunction<Row, Tuple2<Tuple2<String,String>, RoaringBitmap >>() {
            @Override
            public Tuple2<Tuple2<String,String>, RoaringBitmap > map(Row row) throws Exception {

                String rule_id = row.getFieldAs("rule_id");
                byte[] bitmapBytes = row.getFieldAs("profile_users_bitmap");

                // 反序列化本次拿到的规则的bitmap
                RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
                bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));

                return Tuple2.of(Tuple2.of(rule_id,row.getKind() == RowKind.DELETE?"delete":"upsert"), bitmap);
            }
        });

        // 将规则定义流，广播
        MapStateDescriptor<String, RoaringBitmap> broadcastStateDesc = new MapStateDescriptor<>("rule_info", String.class, RoaringBitmap.class);
        BroadcastStream<Tuple2<Tuple2<String, String>, RoaringBitmap>> broadcastStream = ruleDefineStream.broadcast(broadcastStateDesc);


        // 将 广播流与 行为事件流，进行连接
        events
                .keyBy(tp->tp.f0)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Tuple2<String, String>, RoaringBitmap>, String>() {

                    /**
                     * 处理主流的方法
                     */
                    @Override
                    public void processElement(Tuple2<Integer, String> event, KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Tuple2<String, String>, RoaringBitmap>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, RoaringBitmap> broadcastState = ctx.getBroadcastState(broadcastStateDesc);

                        // 对系统内所持有的的每个规则，进行一次人群画像是否包含的判断
                        for (Map.Entry<String, RoaringBitmap> entry : broadcastState.immutableEntries()) {
                            String ruleId = entry.getKey();
                            RoaringBitmap bitmap = entry.getValue();

                            out.collect(String.format("当前行为事件的用户： %d ,  规则： %s 的目标人群是否包含此人： %s" ,event.f0,ruleId,bitmap.contains(event.f0)));
                        }
                    }
                    /**
                     * 处理规则信息流的方法
                     * Tuple2<Tuple2<String, String>, RoaringBitmap>
                     * Tuple2<Tuple2<规则id,操作类型>,人群bitmap>
                     */
                    @Override
                    public void processBroadcastElement(Tuple2<Tuple2<String, String>, RoaringBitmap> ruleInfo, KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Tuple2<String, String>, RoaringBitmap>, String>.Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, RoaringBitmap> broadcastState = ctx.getBroadcastState(broadcastStateDesc);

                        if(ruleInfo.f0.f1.equals("upsert")) {
                            log.error("接收到一个新的规则定义信息，规则id为： {} ",ruleInfo.f0.f0);
                            broadcastState.put(ruleInfo.f0.f0, ruleInfo.f1);
                        }else{
                            log.error("根据规则管理平台的要求，删除了一个规则： {} ",ruleInfo.f0.f0);
                            broadcastState.remove(ruleInfo.f0.f0);
                        }
                    }
                }).print();

        env.execute();
    }
}
