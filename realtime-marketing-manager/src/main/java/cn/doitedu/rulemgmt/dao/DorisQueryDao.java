package cn.doitedu.rulemgmt.dao;

import cn.doitedu.rtmk.common.pojo.ActionSeqParam;
import org.roaringbitmap.RoaringBitmap;

import java.sql.SQLException;

public interface DorisQueryDao {
    // 根据给定的sql来查询行为次数
    void queryActionCount(String sql, String ruleId, String conditionId, RoaringBitmap profileBitmap) throws SQLException;

    void queryActionSeq(String sql, String ruleId, ActionSeqParam actionSeqParam, RoaringBitmap profileBitmap) throws SQLException;
}
