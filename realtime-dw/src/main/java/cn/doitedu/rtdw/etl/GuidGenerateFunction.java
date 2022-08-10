package cn.doitedu.rtdw.etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.*;

public class GuidGenerateFunction extends KeyedProcessFunction<String, EventBean, EventBean> {

    Connection conn;
    PreparedStatement preparedStatement;

    org.apache.hadoop.hbase.client.Connection hbaseConn;

    Table deviceAccountBindTable;
    Table deviceTmpGuidTable;
    Table deviceTmpMaxid;

    MapStateDescriptor accountIdStateDescriptor;
    MapStateDescriptor deviceAccountIdStateDescriptor;
    MapStateDescriptor deviceTmpIdStateDescriptor;

    MapState<String, Tuple2<Long, Long>> accountIdState;
    MapState<String, Tuple3<String, Long, Long>> deviceAccountIdState;
    MapState<String, Long> deviceTmpIdState;


    @Override
    public void open(Configuration parameters) throws Exception {

        // 构造一个jdbc的连接（并不需要用连接池）
        conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai", "root", "root");
        preparedStatement = conn.prepareStatement("select * from ums_member where account = ?");

        // 构造一个hbase的客户端连接
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "doitedu:2181");
        hbaseConn = ConnectionFactory.createConnection(conf);

        // 构造hbase中所要用到的几个表客户端
        deviceAccountBindTable = hbaseConn.getTable(TableName.valueOf("device_account_bind"));
        deviceTmpGuidTable = hbaseConn.getTable(TableName.valueOf("device_tmp_guid"));
        deviceTmpMaxid = hbaseConn.getTable(TableName.valueOf("device_tmp_maxid"));


        // 构造几个用于查询缓存的 state (单值型，list，map，aggreate)
        accountIdStateDescriptor = new MapStateDescriptor<String, Tuple2<Long, Long>>(
                "account-id",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        deviceAccountIdStateDescriptor = new MapStateDescriptor<String, Tuple3<String, Long, Long>>(
                "device-account-id",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple3<String, Long, Long>>() {
                }));

        deviceTmpIdStateDescriptor = new MapStateDescriptor<String, Long>(
                "device-account-id",
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class));

        accountIdState = getRuntimeContext().getMapState(accountIdStateDescriptor);
        deviceAccountIdState = getRuntimeContext().getMapState(deviceAccountIdStateDescriptor);
        deviceTmpIdState = getRuntimeContext().getMapState(deviceTmpIdStateDescriptor);
    }

    /**
     * 填充 guid
     * 填充匿名访客首访时间
     * 填充会员注册时间
     * 填充访客新老属性
     */
    @Override
    public void processElement(EventBean bean, KeyedProcessFunction<String, EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {


        // 一、如果bean中有account
        if (StringUtils.isNotBlank(bean.getAccount())) {
            // 先在state中查找是否已有现成的结果
            Tuple2<Long, Long> accountId = accountIdState.get(bean.getAccount());
            if (accountId != null) {
                bean.setGuid(accountId.f0);
                bean.setRegisterTime(accountId.f1);
            }else{
                // 如果在缓存中没有找到user_id,,则去mysql中查询它的user_id
                preparedStatement.setString(1,bean.getAccount());
                ResultSet resultSet = preparedStatement.executeQuery();

                long userId = -1;
                long register_time = -1;
                while(resultSet.next()){
                    userId = resultSet.getLong("id");
                    register_time = resultSet.getTimestamp("register_time").getTime();
                }

                // 并将该账号对应的user_id及注册时间信息，存入flink state中  : account,user_id,注册时间
                accountIdState.put(bean.getAccount(), Tuple2.of(userId,register_time));

                // 将结果设置到数据bean中
                bean.setGuid(userId);
                bean.setRegisterTime(register_time);

                // 给该账号在 设备-账号绑定表中的权重进行增加,并对该设备的其他绑定账号权重衰减
                /**
                 *   -------------------------------------------------------------------------------------------
                 *   rk        |      f:q                                                                       |
                 *   --------------------------------------------------------------------------------------------
                 *   dev01     | [{"account":"zs","weight":100,"user_id":120,"register_time":17632487346}]      |
                 *   --------------------------------------------------------------------------------------------
                 */
                Result result = deviceAccountBindTable.get(new Get(Bytes.toBytes(bean.getDeviceid())));
                byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
                JSONArray objects = JSON.parseArray(new String(value));

                // 遍历从hbase中取到的所有绑定关系数据
                int size = objects.size();
                for(int i=0;i<size;i++){
                    JSONObject jsonObject = objects.getJSONObject(i);
                    // 取出该账号之前的权重值
                    Float oldWeight = jsonObject.getFloat("weight");

                    // 判断本次遍历到的绑定关系，是否是本次输入数据中的账号
                    if(bean.getAccount().equals(jsonObject.getString("account"))){
                        // 增加权重
                        jsonObject.put("weight",oldWeight+1);
                    }else{
                        // 衰减权重
                        jsonObject.put("weight",oldWeight*0.9);
                    }
                }


            }


        }


        // 如果bean中没有account,则去hbase的设备账号绑定表中查找绑定权重最高的账号，进而得到账号的 user_id
        // 将查询到的结果，存入flink state 中： deviceId,账号,user_id ,注册时间



        // 如果bean中没有account，且在设备账号绑定表中也不存在，则去hbase的 匿名设备id绑定表查找临时guid
        // 将查询到的结果，存入flink state 中  ： 设备id,临时guid

        // 如果 bean中没有account，且在设备账号绑定表中也不存在，且在匿名设备id绑定表也不存在，则去请求guid计数器自增得到一个新的临时guid
        // 将查询到的结果，存入flink state 中


    }


    @Override
    public void close() throws Exception {


    }
}
