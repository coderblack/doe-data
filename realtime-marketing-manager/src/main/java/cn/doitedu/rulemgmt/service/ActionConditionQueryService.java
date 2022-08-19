package cn.doitedu.rulemgmt.service;

import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;

import java.sql.SQLException;

public interface ActionConditionQueryService {
    void queryActionCount(JSONObject eventParamJsonObject, String ruleId, RoaringBitmap profileBitmap) throws SQLException;
}
