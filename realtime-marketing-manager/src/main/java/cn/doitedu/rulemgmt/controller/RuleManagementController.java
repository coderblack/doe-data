package cn.doitedu.rulemgmt.controller;

import cn.doitedu.rulemgmt.dao.DorisQueryDaoImpl;
import cn.doitedu.rulemgmt.dao.RuleSystemMetaDaoImpl;
import cn.doitedu.rulemgmt.service.*;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


/**
 * 前端传入的规则定义参数json结构示例： param_jsons/rule_model_1.json
 */

@RestController
public class RuleManagementController {

    private final ProfileConditionQueryService profileConditionQueryServiceImpl;
    private final ActionConditionQueryService actionConditionQueryService;
    private final RuleSystemMetaService ruleSystemMetaService;

    @Autowired
    public RuleManagementController(
            ProfileConditionQueryService profileConditionQueryServiceImpl,
            ActionConditionQueryService actionConditionQueryService,
            RuleSystemMetaService ruleSystemMetaService) {
        this.profileConditionQueryServiceImpl = profileConditionQueryServiceImpl;
        this.actionConditionQueryService = actionConditionQueryService;
        this.ruleSystemMetaService = ruleSystemMetaService;
    }

    /**
     * 从前端页面接收规则定义的参数json，并发布规则模型01下的规则实例
     * @param ruleDefineJson 规则实例参数定义json
     * @throws IOException
     * @throws SQLException
     */
    @RequestMapping("/api/publish/addrule/model01")
    public void publishRuleModel01(@RequestBody String ruleDefineJson) throws IOException, SQLException {

        System.out.println(ruleDefineJson);

        System.out.println("----------------------------");

        JSONObject ruleDefineJsonObject = JSON.parseObject(ruleDefineJson);
        String ruleId = ruleDefineJsonObject.getString("ruleId");


        /**
         * 一、 人群画像处理
         */
        // 调用service查询满足规则画像条件的人群
        RoaringBitmap bitmap = profileConditionQueryServiceImpl.queryProfileUsers(ruleDefineJsonObject.getJSONArray("profileCondition"));
        System.out.println("人群画像圈选完成： " + bitmap.toString());



        /**
         * 二、 规则的行为条件历史值处理
         */
        // 解析出行为次数条件，到 doris中去查询各条件的历史值，并发布到 redis
        JSONObject actionCountConditionJsonObject = ruleDefineJsonObject.getJSONObject("actionCountCondition");  // 整个规则的参数
        JSONArray eventParamsJsonArray = actionCountConditionJsonObject.getJSONArray("eventParams");  // 事件次数条件的参数

        // 遍历每一个事件次数条件,并进行历史数据查询，且顺便发布到redis
        for(int i = 0;i<eventParamsJsonArray.size(); i++) {
            JSONObject eventParamJsonObject = eventParamsJsonArray.getJSONObject(i);
            // 调用行为条件查询服务，传入行为条件参数，以及人群bitmap
            actionConditionQueryService.queryActionCount(eventParamJsonObject,ruleId,bitmap);
        }
        System.out.println("规则条件历史数据查询发布完成" );

        /**
         * 三、 规则的groovy运算代码处理
         */
        String ruleModelCaculatorGroovyTemplate = ruleSystemMetaService.findRuleModelGroovyTemplate(ruleDefineJsonObject.getInteger("ruleModelId"));
        Template template = Engine.use().getTemplateByString(ruleModelCaculatorGroovyTemplate);


        // 取出规则实例定义参数中的 事件次数条件参数
        JSONObject actionCountCondition = ruleDefineJsonObject.getJSONObject("actionCountCondition");

        // 从事件条件参数中，取出事件条件的个数
        int eventPamramsSize = actionCountCondition.getJSONArray("eventParams").size();

        // 从事件条件参数中，取出事件条件的组合布尔表达式
        String combineExpr = actionCountCondition.getString("combineExpr");

        // 放入一个hashmap中
        HashMap<String, Object> data = new HashMap<>();
        data.put("eventParams", new int[eventPamramsSize]);
        data.put("combineExpr",combineExpr);

        // 利用上面的hashmap，渲染groovy模板，得到最终可执行的groovy代码
        String groovyCaculatorCode = template.renderToString(data);

        /**
         * 四、 正式发布规则，把 3类信息，放入规则平台的元数据库：
         *  1. 人群 bitmap
         *  2. 规则参数（大json）
         *  3. 规则运算的 groovy 代码
         */

        Integer ruleModelId = ruleDefineJsonObject.getInteger("ruleModelId");
        String creatorName = "yao mei";

        ruleSystemMetaService.publishRuleInstance(ruleId,ruleModelId,creatorName,1,bitmap,ruleDefineJson,groovyCaculatorCode);

    }



    /**
     * 从前端页面接收规则定义的参数json，并发布规则模型02下的规则实例
     * @param ruleDefineJson 规则实例参数定义json
     * @throws IOException
     * @throws SQLException
     */
    @RequestMapping("/api/publish/addrule/model02")
    public void publishRuleModel02(@RequestBody String ruleDefineJson) throws IOException, SQLException {

        System.out.println(ruleDefineJson);

        System.out.println("----------------------------");

        JSONObject ruleDefineJsonObject = JSON.parseObject(ruleDefineJson);
        String ruleId = ruleDefineJsonObject.getString("ruleId");


        /**
         * 一、 人群画像处理
         */
        // 调用service查询满足规则画像条件的人群
        RoaringBitmap bitmap = profileConditionQueryServiceImpl.queryProfileUsers(ruleDefineJsonObject.getJSONArray("profileCondition"));
        System.out.println("人群画像圈选完成： " + bitmap.toString());



        /**
         * 二、 规则的行为条件历史值处理
         */
        // 解析出行为次数条件，到 doris中去查询各条件的历史值，并发布到 redis
        JSONObject actionCountConditionJsonObject = ruleDefineJsonObject.getJSONObject("actionCountCondition");  // 整个规则的参数
        JSONArray eventParamsJsonArray = actionCountConditionJsonObject.getJSONArray("eventParams");  // 事件次数条件的参数

        // 遍历每一个事件次数条件,并进行历史数据查询，且顺便发布到redis
        for(int i = 0;i<eventParamsJsonArray.size(); i++) {
            JSONObject eventParamJsonObject = eventParamsJsonArray.getJSONObject(i);
            // 调用行为条件查询服务，传入行为条件参数，以及人群bitmap
            actionConditionQueryService.queryActionCount(eventParamJsonObject,ruleId,bitmap);
        }
        System.out.println("规则条件历史数据查询发布完成" );

        /**
         * 三、 规则的groovy运算代码处理
         */
        String ruleModelCaculatorGroovyTemplate = ruleSystemMetaService.findRuleModelGroovyTemplate(ruleDefineJsonObject.getInteger("ruleModelId"));
        Template template = Engine.use().getTemplateByString(ruleModelCaculatorGroovyTemplate);


        // 取出规则实例定义参数中的 事件次数条件参数
        JSONObject actionCountCondition = ruleDefineJsonObject.getJSONObject("actionCountCondition");

        // 从事件条件参数中，取出事件条件的个数
        int eventPamramsSize = actionCountCondition.getJSONArray("eventParams").size();

        // 从事件条件参数中，取出事件条件的组合布尔表达式
        String combineExpr = actionCountCondition.getString("combineExpr");

        // 放入一个hashmap中
        HashMap<String, Object> data = new HashMap<>();
        data.put("eventParams", new int[eventPamramsSize]);
        data.put("combineExpr",combineExpr);

        // 利用上面的hashmap，渲染groovy模板，得到最终可执行的groovy代码
        String groovyCaculatorCode = template.renderToString(data);

        /**
         * 四、 正式发布规则，把 3类信息，放入规则平台的元数据库：
         *  1. 人群 bitmap
         *  2. 规则参数（大json）
         *  3. 规则运算的 groovy 代码
         */

        Integer ruleModelId = ruleDefineJsonObject.getInteger("ruleModelId");
        String creatorName = "yao mei";

        ruleSystemMetaService.publishRuleInstance(ruleId,ruleModelId,creatorName,1,bitmap,ruleDefineJson,groovyCaculatorCode);

    }





    // 测试： 手动调用controller的规则发布功能
    public static void main(String[] args) throws IOException, SQLException {

        RuleManagementController controller = new RuleManagementController(
                new ProfileConditionQueryServiceImpl(),
                new ActionConditionQueryServiceImpl(new RuleSystemMetaDaoImpl(),
                        new DorisQueryDaoImpl()),new RuleSystemMetaServiceImpl(new RuleSystemMetaDaoImpl()));

        String webFrontJson = "{\n" +
                "   \"ruleId\":\"rule001\",\n" +
                "   \"profileCondition\":[{\"tagId\":\"tg01\",\"compareType\":\"gt\",\"compareValue\":\"1\"},{\"tagId\":\"tg04\",\"compareType\":\"match\",\"compareValue\":\"汽车\"}]\n" +
                "}";

        controller.publishRuleModel01(webFrontJson);

    }

}
