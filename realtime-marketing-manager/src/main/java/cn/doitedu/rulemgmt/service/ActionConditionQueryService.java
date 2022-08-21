package cn.doitedu.rulemgmt.service;

import cn.doitedu.rtmk.common.pojo.ActionSeqParam;
import com.alibaba.fastjson.JSONObject;
import org.roaringbitmap.RoaringBitmap;

import java.sql.SQLException;

public interface ActionConditionQueryService {
    void processActionCountCondition(JSONObject eventParamJsonObject, String ruleId, RoaringBitmap profileBitmap) throws SQLException;

    void processActionSeqCondition(ActionSeqParam actionSeqParam, String ruleId, RoaringBitmap profileBitmap) throws SQLException;
}
