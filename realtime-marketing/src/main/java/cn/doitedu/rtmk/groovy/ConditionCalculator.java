package cn.doitedu.rtmk.groovy;

import cn.doitedu.rulemgmt.pojo.EventBean;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

public interface ConditionCalculator {
    void init(Jedis jedis, JSONObject eventCountConditionParam);

    void calc(EventBean eventBean);

    boolean isMatch(int guid);
}
