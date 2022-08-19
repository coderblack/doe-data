package cn.doitedu.rtmk.common.interfaces;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/19
 * @Desc: 规则运算机的统一接口
 **/
public interface RuleConditionCalculator {

    /**
     * 运算机初始化
     * @param jedis redis客户端
     * @param ruleDefineParamJsonObject 规则参数json对象
     */
    void init(Jedis jedis, JSONObject ruleDefineParamJsonObject);

    /**
     * 规则条件运算逻辑
     * @param userEvent  用户事件
     */
    void calc(UserEvent userEvent);

    /**
     * 规则条件是否满足的判断逻辑
     * @param guid 用户标识
     * @return  是否满足
     */
    boolean isMatch(int guid);
}
