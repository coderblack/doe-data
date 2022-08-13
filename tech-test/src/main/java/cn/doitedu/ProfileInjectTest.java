package cn.doitedu;

import cn.doitedu.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
public class ProfileInjectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("doitedu", 4444);

        DataStream<Event> eventDataStream = Utils.getEventDataStream(ds);

        DataStream<RoaringBitmap> kafkaBitmapStream = Utils.getKafkaBitmap(env);
        MapStateDescriptor<String, RoaringBitmap> broadcastStateDesc = new MapStateDescriptor<>("broadcastStateDesc", TypeInformation.of(String.class), TypeInformation.of(RoaringBitmap.class));
        BroadcastStream<RoaringBitmap> broadcastStream = kafkaBitmapStream.broadcast(broadcastStateDesc);

        eventDataStream
                .keyBy(Event::getUserId)
                .connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, Event, RoaringBitmap, String>() {
                    @Override
                    public void processElement(Event value, KeyedBroadcastProcessFunction<Integer, Event, RoaringBitmap, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 从广播状态取到人群bitmap
                        RoaringBitmap bitmap = ctx.getBroadcastState(broadcastStateDesc).get("rule-1");

                        if(bitmap != null && bitmap.contains(value.getUserId())){
                            out.collect("用户: "+value.getUserId()+" 存在于画像人群中");
                        }else if( bitmap == null){
                            out.collect("bitmap为null");
                        }else {
                            out.collect("用户: "+value.getUserId()+ " 不存在");
                        }
                    }

                    @Override
                    public void processBroadcastElement(RoaringBitmap bitmap, KeyedBroadcastProcessFunction<Integer, Event, RoaringBitmap, String>.Context ctx, Collector<String> out) throws Exception {
                        log.error("收到广播变量");
                        BroadcastState<String, RoaringBitmap> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
                        broadcastState.put("rule-1",bitmap);
                    }
                })
                .print();

        env.execute();
    }
}
