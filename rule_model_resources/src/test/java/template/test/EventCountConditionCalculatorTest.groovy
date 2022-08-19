package cn.doitedu.rtmk.rulemodel.caculator.groovy

import cn.doitedu.rtmk.common.interfaces.RuleConditionCalculator
import cn.doitedu.rtmk.common.pojo.UserEvent
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

class EventCountConditionCalculatorGroovy implements RuleConditionCalculator {

    private Jedis jedis;
    private JSONObject ruleDefineParamJsonObject;
    private JSONObject eventCountConditionParam;
    private String ruleId;

    @Override
    public void init(Jedis jedis, JSONObject ruleDefineParamJsonObject) {
        this.jedis = jedis;
        this.ruleDefineParamJsonObject = ruleDefineParamJsonObject;

        ruleId = ruleDefineParamJsonObject.getString("ruleId");

        this.eventCountConditionParam = ruleDefineParamJsonObject.getJSONObject("actionCountCondition");

    }

    @Override
    public void calc(UserEvent userEvent) {

        JSONArray eventParams = eventCountConditionParam.getJSONArray("eventParams");
        int size = eventParams.size();

        for (int i = 0; i < size; i++) {

            JSONObject eventParam = eventParams.getJSONObject(i);

            Integer conditionId = eventParam.getInteger("conditionId");

            if (userEvent.getEventId().equals(eventParam.getString("eventId"))) {

                JSONArray attributeParams = eventParam.getJSONArray("attributeParams");
                boolean b = judgeEventAttribute(userEvent, attributeParams);

                if (b) {
                    jedis.hincrBy(ruleId + ":" + conditionId, userEvent.getGuid() + "", 1);
                }
            }
        }


    }


    @Override
    public boolean isMatch(int guid) {
        JSONArray eventParams = eventCountConditionParam.getJSONArray("eventParams");


        JSONObject eventParam_0 = eventParams.getJSONObject(0);
        Integer conditionId_0 = eventParam_0.getInteger("conditionId");

        Integer eventCountParam_0 = eventParam_0.getInteger("eventCount");

        String realCountStr_0 = jedis.hget(ruleId + ":" + conditionId_0, guid + "");
        int realCount_0 = Integer.parseInt(realCountStr_0 == null ? "0" : realCountStr_0);

        boolean res_0 = realCount_0 >= eventCountParam_0 ;

        JSONObject eventParam_1 = eventParams.getJSONObject(1);
        Integer conditionId_1 = eventParam_1.getInteger("conditionId");

        Integer eventCountParam_1 = eventParam_1.getInteger("eventCount");

        String realCountStr_1 = jedis.hget(ruleId + ":" + conditionId_1, guid + "");
        int realCount_1 = Integer.parseInt(realCountStr_1 == null ? "0" : realCountStr_1);

        boolean res_1 = realCount_1 >= eventCountParam_1 ;

        JSONObject eventParam_2 = eventParams.getJSONObject(2);
        Integer conditionId_2 = eventParam_2.getInteger("conditionId");

        Integer eventCountParam_2 = eventParam_2.getInteger("eventCount");

        String realCountStr_2 = jedis.hget(ruleId + ":" + conditionId_2, guid + "");
        int realCount_2 = Integer.parseInt(realCountStr_2 == null ? "0" : realCountStr_2);

        boolean res_2 = realCount_2 >= eventCountParam_2 ;




        return   res_0 && (res_1 || res_2) ;
    }

    private boolean judgeEventAttribute(UserEvent userEvent, JSONArray attributeParams) {
        for (int j = 0; j < attributeParams.size(); j++) {
            JSONObject attributeParam = attributeParams.getJSONObject(j);

            String paramAttributeName = attributeParam.getString("attributeName");
            String paramCompareType = attributeParam.getString("compareType");
            String paramValue = attributeParam.getString("compareValue");

            String eventAttributeValue = userEvent.getProperties().get(paramAttributeName);

            if ("=" == paramCompareType && !(paramValue == eventAttributeValue)) {
                return false;
            }

            if (">" == paramCompareType && !(paramValue > eventAttributeValue)) {
                return false;
            }

            if ("<" == paramCompareType && !(paramValue < eventAttributeValue)) {
                return false;
            }

            if ("<=" == paramCompareType && !(paramValue <= eventAttributeValue)) {
                return false;
            }

            if (">=" == paramCompareType && !(paramValue >= eventAttributeValue)) {
                return false;
            }

        }
        return true;
    }


}