package cn.doitedu;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.HashMap;

public interface IActionRuleCalc {
    // 用于初始化状态机
    void init(JSONObject ruleParam, Jedis jedis , HashMap<String,Integer> flinkMapState);

    // 用于对一个用户的一个流入事件进行规则运算
    boolean calc(String userId ,String event);
}
