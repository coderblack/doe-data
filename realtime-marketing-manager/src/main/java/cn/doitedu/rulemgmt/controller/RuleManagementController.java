package cn.doitedu.rulemgmt.controller;

import cn.doitedu.rulemgmt.dao.DorisQueryDaoImpl;
import cn.doitedu.rulemgmt.dao.RuleSystemMetaDaoImpl;
import cn.doitedu.rulemgmt.service.ActionConditionQueryService;
import cn.doitedu.rulemgmt.service.ActionConditionQueryServiceImpl;
import cn.doitedu.rulemgmt.service.ProfileConditionQueryService;
import cn.doitedu.rulemgmt.service.ProfileConditionQueryServiceImpl;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.sql.SQLException;


/**
 * 前端传入的规则定义参数json结构示例： param_jsons/rule_model_1.json
 */

@RestController
public class RuleManagementController {

    private final ProfileConditionQueryService profileConditionQueryServiceImpl;
    private final ActionConditionQueryService actionConditionQueryService;

    @Autowired
    public RuleManagementController(ProfileConditionQueryService profileConditionQueryServiceImpl, ActionConditionQueryService actionConditionQueryService) {
        this.profileConditionQueryServiceImpl = profileConditionQueryServiceImpl;
        this.actionConditionQueryService = actionConditionQueryService;
    }

    // 从前端页面接收规则定义的参数json，并发布规则
    @RequestMapping("/api/publish/addrule")
    public void publishRule(@RequestBody String ruleDefineJson) throws IOException, SQLException {

        System.out.println(ruleDefineJson);

        System.out.println("----------------------------");

        JSONObject ruleDefineJsonObject = JSON.parseObject(ruleDefineJson);
        String ruleId = ruleDefineJsonObject.getString("ruleId");


        System.out.println("------查询画像人群 开始---------");
        // 调用service查询满足规则画像条件的人群
        RoaringBitmap bitmap = profileConditionQueryServiceImpl.queryProfileUsers(ruleDefineJsonObject.getJSONArray("profileCondition"));
        System.out.println(bitmap.contains(3));
        System.out.println(bitmap.contains(5));
        System.out.println("------查询画像人群 完成---------");

        System.out.println("");
        System.out.println("");


        System.out.println("------查询行为次数类条件的历史值 开始---------");
        // 解析出行为次数条件，到 doris中去查询各条件的历史值，并发布到 redis
        JSONObject actionCountConditionJsonObject = ruleDefineJsonObject.getJSONObject("actionCountCondition");  // 整个规则的参数
        JSONArray eventParamsJsonArray = actionCountConditionJsonObject.getJSONArray("eventParams");  // 事件次数条件的参数

        // 遍历每一个事件次数条件,并进行历史数据查询，且顺便发布到redis
        for(int i = 0;i<eventParamsJsonArray.size(); i++) {
            JSONObject eventParamJsonObject = eventParamsJsonArray.getJSONObject(i);
            actionConditionQueryService.queryActionCount(eventParamJsonObject,ruleId);
        }
        System.out.println("------查询行为次数类条件的历史值 开始---------");

        // TODO



    }


    // 测试： 手动调用controller的规则发布功能
    public static void main(String[] args) throws IOException, SQLException {

        RuleManagementController controller = new RuleManagementController(new ProfileConditionQueryServiceImpl(),new ActionConditionQueryServiceImpl(new RuleSystemMetaDaoImpl(),new DorisQueryDaoImpl()));

        String webFrontJson = "{\n" +
                "   \"ruleId\":\"rule001\",\n" +
                "   \"profileCondition\":[{\"tagId\":\"tg01\",\"compareType\":\"gt\",\"compareValue\":\"1\"},{\"tagId\":\"tg04\",\"compareType\":\"match\",\"compareValue\":\"汽车\"}]\n" +
                "}";

        controller.publishRule(webFrontJson);

    }

}
