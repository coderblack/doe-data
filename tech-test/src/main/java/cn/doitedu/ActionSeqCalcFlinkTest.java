package cn.doitedu;

import cn.doitedu.utils.Utils;
import com.alibaba.fastjson.JSONObject;
import groovy.lang.GroovyClassLoader;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.HashMap;



public class ActionSeqCalcFlinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost", 4444);
        DataStream<Event> eventDs = Utils.getEventDataStream(ds);


        SingleOutputStreamOperator<String> res = eventDs.process(new ProcessFunction<Event, String>() {
            IActionRuleCalc calculator;
            JSONObject ruleParam;

            @Override
            public void open(Configuration parameters) throws Exception {
                String scriptText = FileUtils.readFileToString(new File("tech-test/src/main/java/cn/doitedu/groovy/ActionRuleCalc.groovy"));

                // 解析加载字符串形式的groovy状态机类
                GroovyClassLoader classLoader = new GroovyClassLoader();
                Class groovyClass = classLoader.parseClass(scriptText);
                calculator = (IActionRuleCalc) groovyClass.newInstance();

                // 构造redis客户端
                Jedis jedis = new Jedis("doitedu", 6379);

                // 准备规则条件参数
                // {"ruleId":"r01","conditionId":"c01","eventSeq":["A","E","C"],"minCount":1,"maxCount":3}
                String ruleParamStr = "{\"ruleId\":\"r01\",\"conditionId\":\"c01\",\"eventSeq\":[\"A\",\"E\",\"C\"],\"minCount\":3,\"maxCount\":3}";
                ruleParam = JSONObject.parseObject(ruleParamStr);

                // 模拟flink的mapState
                HashMap<String, Integer> flinkMapState = new HashMap<>();

                // 初始化规则条件计算状态机
                calculator.init(ruleParam, jedis, flinkMapState);


            }

            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                boolean res = calculator.calc(event.getUserId()+"", event.getEventId());
                if (res)
                    out.collect(String.format("用户:%s ,规则:%s ,条件:%s ,最小次数:%d ,已满足 ", event.getUserId(), ruleParam.getString("ruleId"), ruleParam.getString("conditionId"), ruleParam.getInteger("minCount")));
            }
        });

        res.print();


        env.execute();

    }


}
