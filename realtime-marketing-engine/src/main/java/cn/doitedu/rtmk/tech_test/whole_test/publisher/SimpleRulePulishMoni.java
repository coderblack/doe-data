package cn.doitedu.rtmk.tech_test.whole_test.publisher;

import cn.doitedu.rtmk.tech_test.whole_test.pojo.EventCountParam;
import cn.doitedu.rtmk.tech_test.whole_test.pojo.PropertyParam;
import cn.doitedu.rtmk.tech_test.whole_test.pojo.RuleInfo;

import java.util.Arrays;
import java.util.Collections;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/16
 * @Desc:  模拟测试规则发布平台的工作流程
 **/
public class SimpleRulePulishMoni {
    public static void main(String[] args) {

        /**
         * 1. 从前端接收到了营销人员定义的规则的条件、参数信息
         */
        RuleInfo ruleInfo = new RuleInfo();

        // 前端传入的规则名称
        ruleInfo.setRuleId("rule_001");


        // 前端传入的触发条件
        EventCountParam triggerEventCondition = new EventCountParam();
        triggerEventCondition.setEventId("sumitOrder");
        ruleInfo.setTriggerEventCondition(triggerEventCondition);

        // 前端传入的画像条件
        PropertyParam tagParam1 = new PropertyParam("tag01", "eq", "C");
        PropertyParam tagParam2 = new PropertyParam("tag03", "lt", "5");
        ruleInfo.setProfileCondition(Arrays.asList(tagParam1,tagParam2));

        // 前端传入的事件次数条件
        EventCountParam eventCountParam = new EventCountParam();
        eventCountParam.setCount(3);
        eventCountParam.setEventId("addcart");
        PropertyParam propParam = new PropertyParam("itemId", "eq", "item01");
        eventCountParam.setPropertyParams(Collections.singletonList(propParam));
        eventCountParam.setWindowStart("2022-08-01 00:00:00");
        eventCountParam.setWindowEnd("2022-08-28 12:00:00");
        eventCountParam.setParamId("1");
        ruleInfo.setEventCountCondition(eventCountParam);


        /**
         * 2 . 根据前端传入的规则的画像条件，去es中圈选人群，并生成bitmap，并填充到  ruleInfo对象中
         */




        /**
         * 3.找到本规则模板对应的groovy运算模型，填充到  ruleInfo对象中
         */




        /**
         * 4. 根据前端传入的规则的事件行为次数受众条件，去doris中查询统计所有用户的结果，并将结果按groovy模板中的数据结构要求，写入redis
         */


    }
}
