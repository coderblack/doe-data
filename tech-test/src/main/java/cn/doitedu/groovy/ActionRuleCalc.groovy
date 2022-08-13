package cn.doitedu.groovy

import cn.doitedu.IActionRuleCalc
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

class ActionRuleCalc implements IActionRuleCalc {

    JSONObject ruleParam
    Jedis jedis
    HashMap<String, Integer> flinkMapState

    String ruleId
    String conditionId
    List<String> eventSeq
    int minCount
    int maxCount


    @Override
    void init(JSONObject ruleParam, Jedis jedis, HashMap<String, Integer> flinkMapState) {
        this.ruleParam = ruleParam
        this.jedis = jedis
        this.flinkMapState = flinkMapState

        this.ruleId = ruleParam.getString("ruleId")
        this.conditionId = ruleParam.getString("conditionId")
        this.eventSeq = ruleParam.getJSONArray("eventSeq").toJavaList(String.class)
        this.minCount = ruleParam.getInteger("minCount")
        this.maxCount = ruleParam.getInteger("maxCount")

    }

    /**
     * 滚动聚合，中间状态数据结构：
     * {"seq":"AE","cnt":1,"cflag":0}
     */
    boolean calc(String userId, String event) {

        if (flinkMapState.get(ruleId + ":" + conditionId + ":" + userId) == 1) return true

        boolean res = false
        String oldSeq = ""
        int oldCnt = 0

        // 先判断，输入事件是否是条件序列所包含的事件
        if (eventSeq.contains(event)) {
            String json = jedis.hget(ruleId + ":" + userId, conditionId)
            JSONObject redisJsonObject

            // 如果redis中已有中间聚合状态（有可能是上线后计算所得，也有可能是发布时历史数据的查询结果）
            if (json != null) {
                redisJsonObject = JSON.parseObject(json)
                oldSeq = redisJsonObject.getString("seq")
                oldCnt = redisJsonObject.getInteger("cnt")
                int cflag = redisJsonObject.getInteger("cflag")

                // 如果该用户该条件在redis中的查询结果是已经满足，则直接返回
                // 主要是为了job重启后且flink内部状态有丢失的情况下，可以快速重建已完成状态
                if(cflag==1) {
                    flinkMapState.put(ruleId + ":" + conditionId + ":" + userId,1)
                    return true
                }
            }
            // 如果redis中没有中间聚合状态，则初始化一个聚合状态
            else {
                redisJsonObject = new JSONObject()
                redisJsonObject.put("seq", oldSeq)
                redisJsonObject.put("cnt", oldCnt)
                redisJsonObject.put("cflag", 0)
            }

            // 如果输入事件，是序列中的下一个目标事件
            if (event == eventSeq.get(oldSeq.length())) {

                // 如果是序列中欠缺的最后一个事件
                if (oldSeq.length() == eventSeq.size() - 1) {

                    // 则发生次数+1 , 序列清空
                    redisJsonObject.put("cnt", oldCnt + 1)
                    redisJsonObject.put("seq", "")

                    res = (oldCnt + 1 >= minCount)

                    // 已经完全满足条件,则将该用户的满足状态直接放入flink state中，以避免无谓的redis查询
                    if (res) {
                        flinkMapState.put(ruleId + ":" + conditionId + ":" + userId, 1)
                        redisJsonObject.put("cflag",1)
                    }
                }

                // 如果是中间事件,则序列追加此事件
                else {
                    redisJsonObject.put("seq", oldSeq + event)
                }

                // 将本次结果写入redis
                jedis.hset(ruleId + ":" + userId, conditionId, redisJsonObject.toJSONString())
            }

        }

        return res
    }
}
