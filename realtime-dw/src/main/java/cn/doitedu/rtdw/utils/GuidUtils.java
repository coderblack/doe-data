package cn.doitedu.rtdw.utils;

import cn.doitedu.rtdw.etl.DeviceAccountBindInfo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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
import java.util.Collections;
import java.util.Comparator;

public class GuidUtils {


    public static Tuple2<Long,Long> getUserIdFromMysql(PreparedStatement preparedStatement,String account) throws SQLException {
        // 如果在缓存中没有找到user_id,,则去mysql中查询它的user_id
        preparedStatement.setString(1,account);
        ResultSet resultSet = preparedStatement.executeQuery();

        long userId = -1;
        long register_time = -1;
        while(resultSet.next()){
            userId = resultSet.getLong("id");
            register_time = resultSet.getTimestamp("register_time").getTime();
        }
        return Tuple2.of(userId,register_time);
    }


    /**
     *  hbase中的  设备账号绑定权重表结构
     *   -------------------------------------------------------------------------------------------
     *   rk        |      f:q                                                                       |
     *   --------------------------------------------------------------------------------------------
     *   dev01     | [{"account":"zs","weight":100,"user_id":120,"register_time":17632487346}]      |
     *   --------------------------------------------------------------------------------------------
     */
    public static JSONArray getDeviceAccountBindInfo(Table table, String deviceId) throws IOException {
        Result result = table.get(new Get(Bytes.toBytes(deviceId)));
        byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
        return JSON.parseArray(new String(value));
    }


    /**
     * 设备账号绑定权重更新处理
     * @param toIncAccount
     * @param deviceId
     * @param table
     * @throws IOException
     */
    public static void deviceAccountBindWeightHandle(String toIncAccount,String deviceId,Table table) throws IOException {

        // 先从hbase中找到目标设备的所有绑定账号权重信息
        JSONArray bindWeights = getDeviceAccountBindInfo(table, deviceId);

        // 遍历从hbase中取到的所有绑定关系数据
        int size = bindWeights.size();
        for(int i=0;i<size;i++){
            JSONObject jsonObject = bindWeights.getJSONObject(i);
            // 取出该账号之前的权重值
            Float oldWeight = jsonObject.getFloat("weight");

            // 判断本次遍历到的绑定关系，是否是本次输入数据中的账号
            if(toIncAccount.equals(jsonObject.getString("account"))){
                // 增加权重
                jsonObject.put("weight",oldWeight+1);
            }else{
                // 衰减权重
                jsonObject.put("weight",oldWeight*0.9);
            }
        }

        // 将修改后的数据，放回hbase
        Put put = new Put(Bytes.toBytes(deviceId));
        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"),Bytes.toBytes(bindWeights.toJSONString()));
        table.put(put);

    }



    public static DeviceAccountBindInfo getMaxBindWeigtAccountInfo(Table table, String deviceId) throws IOException {

        // 找出该设备的所有绑定账号信息
        JSONArray bindInfos = getDeviceAccountBindInfo(table, deviceId);

        // 如果在hbase中没有找到该设备的任何绑定账号信息，直接返回null值
        if(bindInfos.size()<1) return null;

        // 将json数组转成javabean的List
        ArrayList<DeviceAccountBindInfo> lst = new ArrayList<>();

        for(int i=0;i<bindInfos.size();i++){
            // {"account":"zs","weight":100,"user_id":120,"register_time":17632487346}
            JSONObject jsonObject = bindInfos.getJSONObject(i);
            String account = jsonObject.getString("account");
            Float weight = jsonObject.getFloat("weight");
            Long userId = jsonObject.getLong("user_id");
            Long registerTime = jsonObject.getLong("register_time");

            lst.add(new DeviceAccountBindInfo(deviceId,account,weight,userId,registerTime));
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



}
