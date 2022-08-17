package cn.doitedu.rtmk.tech_test.groovytest.groovy

import cn.doitedu.rtmk.tech_test.enjoy_test.EventBean
import cn.doitedu.rtmk.tech_test.enjoy_test.IConditionCalculator
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject

class ConditionCalculator implements IConditionCalculator{
    JSONObject conditionObject;
    JSONArray eventParams;
    public void init(String condition){
        conditionObject = JSON.parseObject(condition)
        eventParams = conditionObject.getJSONArray("eventParams")

    }


    @Override
    boolean calc(EventBean eventBean) {

        JSONObject param0 = eventParams.getJSONObject(0)
        String eventId0 = param0.getString("eventId")
        String attributeName0 = param0.getString("attributeName")
        String operatorType0= param0.getString("operatorType")
        String value0 = param0.getString("value")

        boolean res0 = false;

        if(eventBean.getEventId() == eventId0){
            //println( "json " +  eventId0 + "," + eventBean.getEventId())
            println("oper: " + operatorType0)
            if(operatorType0.equals("eq")){
                res0 = eventBean.getProperties().get(attributeName0) == value0;
            }else if(operatorType0.equals("lt")){
                res0 = eventBean.getProperties().get(attributeName0) > value0;
            }
        }

        println(res0)
        JSONObject param1 = eventParams.getJSONObject(1)
        String eventId1 = param1.getString("eventId")
        String attributeName1 = param1.getString("attributeName")
        String operatorType1= param1.getString("operatorType")
        String value1 = param1.getString("value")

        boolean res1 = false;

        if(eventBean.getEventId() == eventId1){
            //println( "json " +  eventId1 + "," + eventBean.getEventId())
            println("oper: " + operatorType1)
            if(operatorType1.equals("eq")){
                res1 = eventBean.getProperties().get(attributeName1) == value1;
            }else if(operatorType1.equals("lt")){
                res1 = eventBean.getProperties().get(attributeName1) > value1;
            }
        }

        println(res1)
        JSONObject param2 = eventParams.getJSONObject(2)
        String eventId2 = param2.getString("eventId")
        String attributeName2 = param2.getString("attributeName")
        String operatorType2= param2.getString("operatorType")
        String value2 = param2.getString("value")

        boolean res2 = false;

        if(eventBean.getEventId() == eventId2){
            //println( "json " +  eventId2 + "," + eventBean.getEventId())
            println("oper: " + operatorType2)
            if(operatorType2.equals("eq")){
                res2 = eventBean.getProperties().get(attributeName2) == value2;
            }else if(operatorType2.equals("lt")){
                res2 = eventBean.getProperties().get(attributeName2) > value2;
            }
        }

        println(res2)

        boolean res = res0 && ( res1 || res2 )

        return res
    }
}