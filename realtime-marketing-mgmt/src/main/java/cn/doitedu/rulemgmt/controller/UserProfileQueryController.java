package cn.doitedu.rulemgmt.controller;

import cn.doitedu.rulemgmt.service.ProfileConditionQueryServiceImpl;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class UserProfileQueryController {

    ProfileConditionQueryServiceImpl queryService = new ProfileConditionQueryServiceImpl();

    // 手动调用controller的规则发布功能
    public static void main(String[] args) throws IOException {

        UserProfileQueryController controller = new UserProfileQueryController();

        String webFrontJson = "{\n" +
                "   \"ruleId\":\"rule001\",\n" +
                "   \"profileCondition\":[{\"tagId\":\"tg01\",\"compareType\":\"gt\",\"compareValue\":\"1\"},{\"tagId\":\"tg04\",\"compareType\":\"match\",\"compareValue\":\"汽车\"}]\n" +
                "}";

        controller.publishRule(webFrontJson);

    }



    /**
     * 前端传入的规则定义参数json结构示例：
       {
           "ruleId":"rule001",
           "profileCondition":[{"tagId":"tg01","compareType":"eq","compareValue":"3"},{"tagId":"tg04","compareType":"match","compareValue":"运动"}]
       }
     */
    // 从前端页面接收规则定义的参数json，并发布规则
    public void publishRule(String ruleDefineJson) throws IOException {

        // 从规则定义参数中，解析出人群画像条件
        JSONObject ruleDefineJsonObject = JSON.parseObject(ruleDefineJson);

        // 调用service查询满足规则画像条件的人群
        RoaringBitmap bitmap = queryService.queryProfileUsers(ruleDefineJsonObject.getJSONArray("profileCondition"));


        // 解析出行为次数条件，到doris中去查询各条件的历史值，并发布到redis






    }

}
