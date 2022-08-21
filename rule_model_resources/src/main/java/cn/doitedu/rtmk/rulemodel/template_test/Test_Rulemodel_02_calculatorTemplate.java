package cn.doitedu.rtmk.rulemodel.template_test;

import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import groovy.lang.GroovyClassLoader;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.util.HashMap;

public class Test_Rulemodel_02_calculatorTemplate {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {

        String ruleDefineJson = "{\n" +
                "  \"ruleModelId\": \"2\",\n" +
                "  \"ruleId\": \"2-rule001\",\n" +
                "  \"ruleTrigEvent\": {\n" +
                "    \"eventId\": \"e5\",\n" +
                "    \"attributeParams\": [\n" +
                "      {\n" +
                "        \"attributeName\": \"pageId\",\n" +
                "        \"compareType\": \"=\",\n" +
                "        \"compareValue\": \"page001\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "    \"windowEnd\": \"2022-08-30 12:00:00\"\n" +
                "  },\n" +
                "  \"profileCondition\": [\n" +
                "    {\n" +
                "      \"tagId\": \"tg01\",\n" +
                "      \"compareType\": \"gt\",\n" +
                "      \"compareValue\": \"2\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"tagId\": \"tg04\",\n" +
                "      \"compareType\": \"match\",\n" +
                "      \"compareValue\": \"汽车\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"actionCountCondition\": {\n" +
                "    \"eventParams\": [\n" +
                "      {\n" +
                "        \"eventId\": \"e1\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 3,\n" +
                "        \"conditionId\": 1,\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e3\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"attributeName\": \"itemId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"item003\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 1,\n" +
                "        \"conditionId\": 2,\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e2\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page002\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "        \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "        \"eventCount\": 2,\n" +
                "        \"conditionId\": 3,\n" +
                "        \"dorisQueryTemplate\": \"action_count\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"combineExpr\": \" res_0 && res_1 && res_2 \"\n" +
                "  },\n" +
                "  \"actionSeqCondition\": {\n" +
                "    \"eventParams\": [\n" +
                "      {\n" +
                "        \"eventId\": \"e1\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e3\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"attributeName\": \"itemId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"item003\"\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"eventId\": \"e2\",\n" +
                "        \"attributeParams\": [\n" +
                "          {\n" +
                "            \"attributeName\": \"pageId\",\n" +
                "            \"compareType\": \"=\",\n" +
                "            \"compareValue\": \"page001\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    ],\n" +
                "    \"windowStart\": \"2022-08-01 12:00:00\",\n" +
                "    \"windowEnd\": \"2022-08-30 12:00:00\",\n" +
                "    \"conditionId\": 4,\n" +
                "    \"dorisQueryTemplate\": \"action_seq\",\n" +
                "    \"seqCount\": 2\n" +
                "  },\n" +
                "  \"rule_match_count\": 2 ,\n" +
                "  \"combineExpr\" : \"res_0 && res_1\"\n" +
                "}";




        Template template = Engine.use().getTemplate("D:\\IdeaProjects\\doe-data\\rule_model_resources\\templates\\rule_calculator\\rulemodel_02_caculator.enjoy");

        HashMap<String, Object> data = new HashMap<>();

        JSONObject ruleDefineJsonObject = JSON.parseObject(ruleDefineJson);
        JSONObject actionCountCondition = ruleDefineJsonObject.getJSONObject("actionCountCondition");

        // 事件条件的个数
        int eventPamramsSize = actionCountCondition.getJSONArray("eventParams").size();

        // 事件条件的组合布尔表达式
        String cntConditionCombineExpr = actionCountCondition.getString("combineExpr");

        data.put("eventParams", new int[eventPamramsSize]);
        data.put("cntConditionCombineExpr",cntConditionCombineExpr);
        data.put("ruleCombineExpr",ruleDefineJsonObject.getString("combineExpr"));

        // 渲染groovy代码
        String code = template.renderToString(data);
        System.out.println(code);

        System.out.println("----------------编译加载代码，进行调用---------------------------");
        Jedis jedis = new Jedis("doitedu", 6379);


        Class aClass = new GroovyClassLoader().parseClass(code);
        RuleCalculator caculator = (RuleCalculator) aClass.newInstance();
        // 先初始化
        caculator.init(jedis,ruleDefineJsonObject, RoaringBitmap.bitmapOf(1,2,3,4,5),null);

        /**
         * 行为次数条件，测试规则参数：
         *   e1,  p1=v1  ,>=3
         *   e2,  p1=v2,p2=v3 ,>=1
         *   e3,  p1=v1  ,>=2
         *
         *  => res_0 && (res_1 || res_2 )
         *
         *  行为序列条件，测试参数：
         *    e1 page001
         *    e3 item003 page001
         *    e2 page001
         */
        // 造一个用户事件
        HashMap<String, String> properties = new HashMap<>();
        properties.put("pageId","page001");
        properties.put("p2","v3");
        UserEvent e1 = new UserEvent(1, "e1", properties, 1661046973000L);

        HashMap<String, String> properties3 = new HashMap<>();
        properties3.put("pageId","page001");
        properties3.put("itemId","item003");
        UserEvent e3 = new UserEvent(1, "e3", properties3, 1661046973000L);


        HashMap<String, String> properties2 = new HashMap<>();
        properties2.put("pageId","page002");
        properties2.put("p2","v3");
        UserEvent e2 = new UserEvent(1, "e2", properties2, 1661046973000L);


        caculator.process(e1);
        caculator.process(e3);
        caculator.process(e2);




        // 调用运算机进行运算
        // 调用1000次进行性能测试
        /*long start = System.currentTimeMillis();
        for(int i=0;i<1;i++) {
            caculator.process(e1);

            if(i % 10 == 0 ) {
                caculator.calc(e1);  // 1/10概率符合规则参数要求
            }else{
                caculator.calc(e5);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);  // 耗时约159ms*/


        // 然后做匹配判断
        System.out.println(caculator.isMatch(1));




    }

}
