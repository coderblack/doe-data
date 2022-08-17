package cn.doitedu.rulemgmt.dao;

import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.HashMap;

@Repository
public class DorisQueryDao {
    Connection conn;
    Statement statement;
    Jedis jedis;

    public DorisQueryDao() throws SQLException {
        conn = DriverManager.getConnection("jdbc:mysql://doitedu:9030/test", "root", "");
        statement = conn.createStatement();

        jedis = new Jedis("doitedu", 6379);
    }


    // 根据给定的sql来查询行为次数
    public void queryActionCount(String sql,String ruleId,String conditionId) throws SQLException {

        ResultSet resultSet = statement.executeQuery(sql);

        HashMap<String, String> guidAndCount = new HashMap<>();
        while(resultSet.next()){
            int guid = resultSet.getInt("guid");
            long cnt = resultSet.getLong("cnt");
            guidAndCount.put(guid+"",cnt+"");
        }


        // 将查询的事件次数条件结果，发布到规则引擎用的redis存储中，作为未来滚动运算的初始值
        // ruleId:conditionId ->  HASH [guid->cnt]
        jedis.hmset(ruleId+":"+conditionId,guidAndCount);


    }

    // 测试本类
    public static void main(String[] args) throws SQLException {

        DorisQueryDao dorisQueryDao = new DorisQueryDao();
        dorisQueryDao.queryActionCount("SELECT\n" +
                "   guid,\n" +
                "   count(1) as cnt\n" +
                "FROM mall_app_events_detail\n" +
                "WHERE 1=1 \n" +
                "AND event_time>='2022-08-01 10:00:00' \n" +
                "AND event_time<='2022-08-31 12:00:00'\n" +
                "AND event_id = 'e2'\n" +
                "AND get_json_string(propJson,'$.pageId') = 'page002'\n" +
                "GROUP BY guid","rule01","1");

    }

}
