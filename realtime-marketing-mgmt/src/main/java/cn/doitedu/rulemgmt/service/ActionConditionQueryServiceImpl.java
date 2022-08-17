package cn.doitedu.rulemgmt.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ActionConditionQueryServiceImpl {

    /**
     {
     "eventId":"share",
     "attributeParams":[
     {
     "attributeName":"pageId",
     "compareType":"eq",
     "compareValue":"page001"
     },
     {
     "attributeName":"itemId",
     "compareType":"eq",
     "compareValue":"item002"
     }
     ],
     "windowStart":"2022-08-01 12:00:00",
     "windowEnd":"2022-08-30 12:00:00",
     "eventCount":3,
     "conditionId":1
     }
     */
    public void queryActionCount(JSONObject actionConditionParamJsonObject){

        JSONArray attributeParams = actionConditionParamJsonObject.getJSONArray("attributeParams");

        String sql = "SELECT\n" +
                "   guid,\n" +
                "   count(1) as cnt\n" +
                "FROM mall_app_events_detail\n" +
                "where envet_time BETWEEN \"2022-08-01 12:00:00\" AND \"2022-08-30 12:00:00\"\n" +
                "AND event_id = 'share' \n" +
                "AND get_json_string(propJson,'$.pageId') = 'page001'\n" +
                "AND get_json_string(propJson,'$.itemId') = 'item002'\n" +
                "GROUP BY guid";


        String.format(" i am %s ,my age is %d","taoge",18); // i am taoge  ,my age is 18


    }


}
