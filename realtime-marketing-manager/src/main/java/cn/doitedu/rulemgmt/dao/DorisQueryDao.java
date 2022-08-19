package cn.doitedu.rulemgmt.dao;

import java.sql.SQLException;

public interface DorisQueryDao {
    // 根据给定的sql来查询行为次数
    void queryActionCount(String sql, String ruleId, String conditionId) throws SQLException;
}
