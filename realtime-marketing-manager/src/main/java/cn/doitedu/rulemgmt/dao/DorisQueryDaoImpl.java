package cn.doitedu.rulemgmt.dao;

import cn.doitedu.rtmk.common.pojo.ActionSeqParam;
import cn.doitedu.rtmk.common.pojo.EventParam;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.*;

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

        while (resultSet.next()) {
            int guid = resultSet.getInt("guid");
            // 属于画像人群的用户，才把它的历史行为次数条件查询结果放入redis
            if (profileBitmap.contains(guid)) {
                long cnt = resultSet.getLong("cnt");
                guidAndCount.put(guid + "", cnt + "");

                // 将查询的事件次数条件结果，攒够一批次，发布到规则引擎用的redis存储中，作为未来滚动运算的初始值
                if (guidAndCount.size() == 1000) {
                    jedis.hmset(ruleId + ":" + conditionId, guidAndCount);
                    guidAndCount.clear();
                }
            }
        }

        // 将查最后不满1000的批次，发布到规则引擎用的redis存储中
        // ruleId:conditionId ->  HASH [guid->cnt]
        if (guidAndCount.size() > 0) {
            jedis.hmset(ruleId + ":" + conditionId, guidAndCount);
        }

    }

    @Override
    public void queryActionSeq(String sql, String ruleId, ActionSeqParam actionSeqParam, RoaringBitmap profileBitmap) throws SQLException {

        List<EventParam> eventParams = actionSeqParam.getEventParams();

        String redisSeqStepKey = ruleId + ":" + actionSeqParam.getConditionId() + ":step";
        String redisSeqCntKey = ruleId + ":" + actionSeqParam.getConditionId() + ":cnt";

        //  3,"e2_2022-08-01 14:32:35,e3_2022-08-01 14:33:35,e1_2022-08-01 14:34:35"
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            int guid = resultSet.getInt(1);
            if (profileBitmap.contains(guid)) {
                String actionSeqStr = resultSet.getString(2);
                //[e2_2022-08-01 14:32:35^e3_2022-08-01 14:33:35^e1_2022-08-01 14:34:35]
                String[] split = actionSeqStr.split("\\^");
                // 对事件序列按时间顺序排序
                Arrays.sort(split, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        String[] o1Split = o1.split("_");
                        String[] o2Split = o2.split("_");
                        return o1Split[1].compareTo(o2Split[1]);
                    }
                });

                // 条件序列中的比较位置
                int k = 0;
                int matchCount = 0;

                // 遍历查询出来的行为序列
                for (int i = 0; i < split.length; i++) {
                    if (split[i].split("_")[0].equals(eventParams.get(k).getEventId())) {
                        k++;
                        if (k == eventParams.size()) {
                            k = 0;
                            matchCount++;
                        }
                    }
                }

                // 往redis插入该用户的待完成序列的已到达步骤号
                jedis.hset(redisSeqStepKey, guid + "", k + "");
                // 往redis插入该用户的序列条件的已完成次数
                jedis.hset(redisSeqCntKey, guid + "", matchCount + "");
            }

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
                "GROUP BY guid", "rule01", "1", RoaringBitmap.bitmapOf(1, 3, 5, 6));

    }

}
