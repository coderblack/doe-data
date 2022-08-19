package cn.doitedu.rulemgmt.service;

import cn.doitedu.rulemgmt.dao.DorisQueryDaoImpl;
import cn.doitedu.rulemgmt.dao.RuleSystemMetaDaoImpl;
import cn.doitedu.rulemgmt.pojo.ActionAttributeParam;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
/**
 {
 "eventId":"share",
 "attributeParams":[
 {
 "attributeName":"pageId",
 "compareType":"=",
 "compareValue":"page001"
 },
 {
 "attributeName":"itemId",
 "compareType":">",
 "compareValue":"item002"
 }
 ],
 "windowStart":"2022-08-01 12:00:00",
 "windowEnd":"2022-08-30 12:00:00",
 "eventCount":3,
 "conditionId":1,
 "dorisQueryTemplate":"action_count"
 }
 */
@Service
public class ActionConditionQueryServiceImpl implements ActionConditionQueryService {

    private final RuleSystemMetaDaoImpl ruleSystemMetaDao;
    private final DorisQueryDaoImpl dorisQueryDaoImpl;

    @Autowired
    public ActionConditionQueryServiceImpl(RuleSystemMetaDaoImpl ruleSystemMetaDao, DorisQueryDaoImpl dorisQueryDaoImpl) {
        this.ruleSystemMetaDao = ruleSystemMetaDao;
        this.dorisQueryDaoImpl = dorisQueryDaoImpl;
    }

    @Override
    public void queryActionCount(JSONObject eventParamJsonObject, String ruleId, RoaringBitmap profileBitmap) throws SQLException {

        // 从事件次数条件中，取出各条件参数
        String eventId = eventParamJsonObject.getString("eventId");
        String windowStart = eventParamJsonObject.getString("windowStart");
        String windowEnd = eventParamJsonObject.getString("windowEnd");
        Integer conditionId = eventParamJsonObject.getInteger("conditionId"); // 条件id

        JSONArray attributeParamsJsonArray = eventParamJsonObject.getJSONArray("attributeParams");

        // 将事件属性参数，封装到一个list中
        ArrayList<ActionAttributeParam> attrParamList = new ArrayList<>();
        for(int i=0;i<attributeParamsJsonArray.size();i++){
            JSONObject paramJsonObject = attributeParamsJsonArray.getJSONObject(i);
            ActionAttributeParam param = new ActionAttributeParam(paramJsonObject.getString("attributeName"), paramJsonObject.getString("compareType"), paramJsonObject.getString("compareValue"));
            attrParamList.add(param);
        }

        // 构造模板渲染用的数据封装
        HashMap<String, Object> data = new HashMap<>();
        data.put("eventId",eventId);
        data.put("windowStart",windowStart);
        data.put("windowEnd",windowEnd);
        data.put("attrParamList",attrParamList);

        // 调用 dao类，来查询规则系统的元数据库中，行为次数条件所对应的doris查询sql模板
        String sqlTemplateStr = ruleSystemMetaDao.getSqlTemplateByTemplateName(eventParamJsonObject.getString("dorisQueryTemplate"));

        // 利用enjoy模板引擎，通过规则参数，来动态拼接真正的查询sql
        Template template = Engine.use().getTemplateByString(sqlTemplateStr);
        String sql = template.renderToString(data);

        // 调用doris查询dao，去执行这个sql，得到结果
        dorisQueryDaoImpl.queryActionCount(sql,ruleId,conditionId+"" ,profileBitmap);


    }


    public static void main(String[] args) throws SQLException {

        ActionConditionQueryServiceImpl service = new ActionConditionQueryServiceImpl(new RuleSystemMetaDaoImpl(),new DorisQueryDaoImpl());
        String conditionJson = "  {\n" +
                " \"eventId\":\"share\",\n" +
                " \"attributeParams\":[\n" +
                " {\n" +
                " \"attributeName\":\"pageId\",\n" +
                " \"compareType\":\"=\",\n" +
                " \"compareValue\":\"page001\"\n" +
                " },\n" +
                " {\n" +
                " \"attributeName\":\"itemId\",\n" +
                " \"compareType\":\">\",\n" +
                " \"compareValue\":\"item002\"\n" +
                " }\n" +
                " ],\n" +
                " \"windowStart\":\"2022-08-01 12:00:00\",\n" +
                " \"windowEnd\":\"2022-08-30 12:00:00\",\n" +
                " \"eventCount\":3,\n" +
                " \"conditionId\":1,\n" +
                " \"dorisQueryTemplate\":\"action_count\"\n" +
                " }";

        JSONObject jsonObject = JSON.parseObject(conditionJson);

        service.queryActionCount(jsonObject,"rule001",RoaringBitmap.bitmapOf(1,2,3,4));


    }


}
