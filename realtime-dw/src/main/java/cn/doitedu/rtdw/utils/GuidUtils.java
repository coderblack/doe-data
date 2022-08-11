package cn.doitedu.rtdw.utils;

import cn.doitedu.rtdw.etl.pojo.DeviceAccountBindInfo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;

public class GuidUtils {


    public static Tuple2<Long, Long> getUserIdFromMysql(PreparedStatement preparedStatement, String account) throws SQLException {
        // 如果在缓存中没有找到user_id,,则去mysql中查询它的user_id
        preparedStatement.setString(1, account);
        ResultSet resultSet = preparedStatement.executeQuery();

        long userId = -1;
        long register_time = -1;
        while (resultSet.next()) {
            userId = resultSet.getLong("id");
            register_time = resultSet.getTimestamp("register_time").getTime();
        }
        return Tuple2.of(userId, register_time);
    }


    /**
     * hbase中的  设备账号绑定权重表结构
     * -------------------------------------------------------------------------------------------
     * rk        |      f:q                                                                       |
     * --------------------------------------------------------------------------------------------
     * dev01     | [{"account":"zs","weight":100,"user_id":120,"register_time":17632487346}]      |
     * --------------------------------------------------------------------------------------------
     */
    public static JSONArray getDeviceAccountBindInfo(Table table, String deviceId) throws IOException {
        Result result = table.get(new Get(Bytes.toBytes(deviceId)));
        byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
        if (value != null) {
            return JSON.parseArray(new String(value));
        }else{
            return null;
        }
    }


    /**
     * 设备账号绑定权重更新处理
     *
     * @param toIncAccount
     * @param deviceId
     * @param table
     * @throws IOException
     */
    public static void deviceAccountBindWeightHandle(String toIncAccount, String deviceId, Table table ,long userId ,long registerTime) throws IOException {

        // 先从hbase中找到目标设备的所有绑定账号权重信息
        JSONArray bindWeights = getDeviceAccountBindInfo(table, deviceId);
        if(bindWeights == null ){
            // [{"account":"zs","weight":100,"user_id":120,"register_time":17632487346}]
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("account",toIncAccount);
            jsonObject.put("weight",100);
            jsonObject.put("user_id",userId);
            jsonObject.put("register_time",registerTime);

            JSONArray objects = new JSONArray();
            objects.add(jsonObject);


            // 将新构造的设备-账号绑定信息，放入hbase表
            Put put = new Put(Bytes.toBytes(deviceId));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(objects.toJSONString()));
            table.put(put);

        }else {

            // 遍历从hbase中取到的所有绑定关系数据
            int size = bindWeights.size();
            for (int i = 0; i < size; i++) {
                JSONObject jsonObject = bindWeights.getJSONObject(i);
                // 取出该账号之前的权重值
                Float oldWeight = jsonObject.getFloat("weight");

                // 判断本次遍历到的绑定关系，是否是本次输入数据中的账号
                if (toIncAccount.equals(jsonObject.getString("account"))) {
                    // 增加权重
                    jsonObject.put("weight", oldWeight + 1);
                } else {
                    // 衰减权重
                    jsonObject.put("weight", oldWeight * 0.9);
                }
            }

            // 将修改后的数据，放回hbase
            Put put = new Put(Bytes.toBytes(deviceId));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(bindWeights.toJSONString()));
            table.put(put);

        }


    }


    /**
     * 查找一个设备的绑定权重最大的账号信息
     *
     * @param table
     * @param deviceId
     * @return
     * @throws IOException
     */
    public static DeviceAccountBindInfo getMaxBindWeigtAccountInfo(Table table, String deviceId) throws IOException {

        // 找出该设备的所有绑定账号信息
        JSONArray bindInfos = getDeviceAccountBindInfo(table, deviceId);

        // 如果在hbase中没有找到该设备的任何绑定账号信息，直接返回null值
        if (bindInfos == null || bindInfos.size() < 1) return null;

        // 将json数组转成javabean的List
        ArrayList<DeviceAccountBindInfo> lst = new ArrayList<>();

        for (int i = 0; i < bindInfos.size(); i++) {
            // {"account":"zs","weight":100,"user_id":120,"register_time":17632487346}
            JSONObject jsonObject = bindInfos.getJSONObject(i);
            String account = jsonObject.getString("account");
            Float weight = jsonObject.getFloat("weight");
            Long userId = jsonObject.getLong("user_id");
            Long registerTime = jsonObject.getLong("register_time");

            lst.add(new DeviceAccountBindInfo(deviceId, account, weight, userId, registerTime));
        }


        // 按权重降序排序，按注册时间倒序排序
        lst.sort(new Comparator<DeviceAccountBindInfo>() {
            @Override
            public int compare(DeviceAccountBindInfo o1, DeviceAccountBindInfo o2) {
                return o2.getWeight().compareTo(o1.getWeight()) == 0 ? o2.getRegisterTime().compareTo(o1.getRegisterTime()) : o2.getWeight().compareTo(o1.getWeight());
            }
        });


        // 取排序后的list中的第一个账号绑定信息
        return lst.get(0);

    }


    /**
     * 根据设备id，查找它的匿名映射临时id
     *
     * @param table
     * @param deviceId
     * @return * -------------------------------------------------------------
     * * rk      |      f:q                                            |
     * * ------------------------------------------------------------
     * * dev01   | {tmpId:1001,ft:1374985872833,lt:1374998809889234} |
     * * ------------------------------------------------------------
     */
    public static Tuple2<Long, Long> getDeviceTmpIdFromHbase(Table table, String deviceId) throws IOException {
        Result result = table.get(new Get(Bytes.toBytes(deviceId)));
        if (!result.isEmpty()) {
            byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
            JSONObject jsonObject = JSON.parseObject(new String(value));
            Long tmpId = jsonObject.getLong("tmpId");
            Long firstAccessTime = jsonObject.getLong("ft");

            return Tuple2.of(tmpId, firstAccessTime);

        } else {

            return null;
        }
    }

    /**
     * 新增一个设备和匿名临时id到hbase表
     *
     * @param table
     * @param deviceId
     * @param newTmpId
     * @param firstAccessTime
     */
    public static void addDeviceTmpIdToHbase(Table table, String deviceId, Long newTmpId, Long firstAccessTime) throws IOException {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("tmpId", newTmpId);
        jsonObject.put("ft", firstAccessTime);

        Put put = new Put(Bytes.toBytes(deviceId));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(jsonObject.toJSONString()));

        table.put(put);

    }


    /**
     * 向hbase的计数器表，发送递增命令，得到新的临时id
     * ----------------------------------------------------
     * rk      |      f:q                        |
     * ------------------------------------------------------
     * r      |    1000001                      |
     * -------------------------------------------------------
     *
     * @param table
     * @return
     */
    public static Long getNewTmpId(Table table) throws IOException {

        long newTmpId = table.incrementColumnValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"), 1);

        return newTmpId;

    }


}
