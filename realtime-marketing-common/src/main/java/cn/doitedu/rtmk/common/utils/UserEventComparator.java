package cn.doitedu.rtmk.common.utils;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class UserEventComparator {

    public static boolean userEventIsEqualParam(UserEvent userEvent, JSONObject eventParam){
        String eventIdParam = eventParam.getString("eventId");
        JSONArray attributeParams = eventParam.getJSONArray("attributeParams");

        if(eventIdParam.equals(userEvent.getEventId())) {
            // 对每一个属性条件进行判断
            for (int j = 0; j < attributeParams.size(); j++) {
                // 取出一个属性参数
                JSONObject attributeParam = attributeParams.getJSONObject(j);

                String paramAttributeName = attributeParam.getString("attributeName");
                String paramCompareType = attributeParam.getString("compareType");
                String paramValue = attributeParam.getString("compareValue");

                String eventAttributeValue = userEvent.getProperties().get(paramAttributeName);

                if ("=".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue)==0)) {
                    return false;
                }

                if (">".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue)>0)) {
                    return false;
                }

                if ("<".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue)<0)) {
                    return false;
                }

                if ("<=".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue)<=0)) {
                    return false;
                }

                if (">=".equals(paramCompareType) && !(paramValue.compareTo(eventAttributeValue)>=0)) {
                    return false;
                }

            }
            return true;
        }

        return false;
    }
}
