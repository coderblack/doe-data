package cn.doitedu.rulemgmt.service;

import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;

public interface ActionConditionQueryService {
    void queryActionCount(JSONObject eventParamJsonObject, String ruleId) throws SQLException;
}
