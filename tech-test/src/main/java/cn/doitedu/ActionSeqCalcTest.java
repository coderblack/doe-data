package cn.doitedu;

import com.alibaba.fastjson.JSONObject;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.io.FileUtils;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ActionSeqCalcTest {
    public static void main(String[] args) throws Exception {
        String scriptText = FileUtils.readFileToString(new File("tech-test/src/main/java/cn/doitedu/groovy/ActionRuleCalc.groovy"));

        // 解析加载字符串形式的groovy状态机类
        GroovyClassLoader classLoader = new GroovyClassLoader();
        Class groovyClass = classLoader.parseClass(scriptText);
        IActionRuleCalc calculator = (IActionRuleCalc) groovyClass.newInstance();

        // 构造redis客户端
        Jedis jedis = new Jedis("doitedu", 6379);

        // 准备规则条件参数
        // {"ruleId":"r01","conditionId":"c01","eventSeq":["A","E","C"],"minCount":1,"maxCount":3}
        String ruleParamStr = "{\"ruleId\":\"r01\",\"conditionId\":\"c01\",\"eventSeq\":[\"A\",\"E\",\"C\"],\"minCount\":3,\"maxCount\":3}";
        JSONObject ruleParam = JSONObject.parseObject(ruleParamStr);

        // 模拟flink的mapState
        HashMap<String, Integer> flinkMapState = new HashMap<>();


        // 初始化规则条件计算状态机
        calculator.init(ruleParam,jedis,flinkMapState);

        // 模拟用户事件流片段,片段中刚好包含一次完整条件序列
        List<String> strings = Arrays.asList("X", "Y", "A", "X", "Y","X", "Y", "E", "E", "X", "Y","A", "C");

        // 将模拟事件流重复10000次，调用状态机进行规则运算，并测试性能
        boolean res = false;
        long start = System.currentTimeMillis();
        for(int i=0;i<10000;i++) {
            for (String event : strings) {
                res = calculator.calc("u01", event);
            }
        }
        long end = System.currentTimeMillis();

        // 输出总耗时
        System.out.println(end-start);

        // 最终匹配结果
        System.out.println(res);

    }
}
