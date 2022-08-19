package cn.doitedu.rulemgmt.service;

import com.alibaba.fastjson.JSONArray;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public interface ProfileConditionQueryService {
    // 接口文档：
    // [{"tagId":"tg01","compareType":"eq","compareValue":"3"},{"tagId":"tg04","compareType":"match","compareValue":"运动"}]
    RoaringBitmap queryProfileUsers(JSONArray jsonArray) throws IOException;
}
