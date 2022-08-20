package cn.doitedu.rulemgmt.dao;

import org.roaringbitmap.RoaringBitmap;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.HashMap;

@Repository
public class DorisQueryDaoImpl implements DorisQueryDao {
    Connection conn;
    Statement statement;
    Jedis jedis;

    public DorisQueryDaoImpl() throws SQLException {
        conn = DriverManager.getConnection("jdbc:mysql://doitedu:9030/test", "root", "");
        statement = conn.createStatement();

        jedis = new Jedis("doitedu", 6379);
    }


    // 根据给定的sql来查询行为次数
    @Override
    public void queryActionCount(String sql, String ruleId, String conditionId, RoaringBitmap profileBitmap) throws SQLException {

        ResultSet resultSet = statement.executeQuery(sql);

        HashMap<String, String> guidAndCount = new HashMap<>();

        while(resultSet.next()){
            int guid = resultSet.getInt("guid");
            // 属于画像人群的用户，才把它的历史行为次数条件查询结果放入redis
            if(profileBitmap.contains(guid)) {
                long cnt = resultSet.getLong("cnt");
                guidAndCount.put(guid + "", cnt + "");

                // 将查询的事件次数条件结果，攒够一批次，发布到规则引擎用的redis存储中，作为未来滚动运算的初始值
                if(guidAndCount.size()==1000){
                    jedis.hmset(ruleId+":"+conditionId,guidAndCount);
                    guidAndCount.clear();
                }
            }
        }

        // 将查最后不满1000的批次，发布到规则引擎用的redis存储中
        // ruleId:conditionId ->  HASH [guid->cnt]
        if(guidAndCount.size()>0) {
            jedis.hmset(ruleId + ":" + conditionId, guidAndCount);
        }

    }

    // 测试本类
    public static void main(String[] args) throws SQLException {

        DorisQueryDaoImpl dorisQueryDaoImpl = new DorisQueryDaoImpl();
        dorisQueryDaoImpl.queryActionCount("SELECT\n" +
                "   guid,\n" +
                "   count(1) as cnt\n" +
                "FROM mall_app_events_detail\n" +
                "WHERE 1=1 \n" +
                "AND event_time>='2022-08-01 10:00:00' \n" +
                "AND event_time<='2022-08-31 12:00:00'\n" +
                "AND event_id = 'e2'\n" +
                "AND get_json_string(propJson,'$.pageId') = 'page002'\n" +
                "GROUP BY guid","rule01","1",RoaringBitmap.bitmapOf(1,3,5,6));

    }

}
