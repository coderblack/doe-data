package cn.doitedu.rtdw.etl.functions;

import cn.doitedu.rtdw.etl.pojo.DeviceAccountBindInfo;
import cn.doitedu.rtdw.etl.pojo.EventBean;
import cn.doitedu.rtdw.utils.GuidUtils;
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
import org.apache.hadoop.hbase.client.Table;

import java.sql.*;


/**
 * -- 测试用 mysql 用户注册信息表：
 CREATE TABLE `ums_member` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `account` varchar(255) DEFAULT NULL,
 `create_time` bigint(20) DEFAULT NULL,
 `update_time` bigint(20) DEFAULT NULL,
 PRIMARY KEY (`id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;



 * -- 设备账号绑定权重关系表，hbase创建
 * hbase>  create 'device_account_bind','f'
 *
 * -- 表结构说明
 * rowKey:deviceId
 * family: "f"
 * qualifier : 账号
 * value:  评分
 * ------------------------------------------------
 * rk        |      f                             |
 * -----------------------------------------------
 * dev01     | ac01:100:user_id:注册时间, ac02:80:user_id:注册时间|
 * ----------------------------------------------

 * -- 设备临时GUID表，hbase创建
 * hbase>  create 'device_tmp_guid','f'
 * -- 表结构说明
 * rowKey:deviceId
 * family: "f"
 * qualifier : "guid"
 * value:  100000001
 * qualifier: ft  -- 首次到访时间
 * value:  137498587283123
 * -------------------------------------------------------------
 * rk      |      f:q                                            |
 * ------------------------------------------------------------
 * dev01     | {guid:1001,ft:1374985872833,lt:1374998809889234} |
 * ------------------------------------------------------------


 * -- 设备临时GUID计数器表，hbase创建
 * hbase>  create 'device_tmp_maxid','f'
 *
 * -- 表结构说明
 * rowKey: "r"
 * family: "f"
 * qualifier : "maxid"
 * value:  100000001
 *
 * ----------------------------------------------------
 * rk      |      f                        |
 * ------------------------------------------------------
 * r      | maxId:1000001                 |
 * -------------------------------------------------------
 *
 * -- 放入1亿这个初始值的客户端命令
 * incr 'device_tmp_maxid','r','f:maxid',100000000
 *
 *
 * -- 创建hbase中的geohash地域信息维表
 * hbase>  create 'dim_geo_area','f'
 * -- 数据结构说明
 * rowkey:  geohash码
 * family:  f
 * qualifier: area
 * ----------------------------------------------------
 * rk         |      f                              |
 * ------------------------------------------------------
 * a2354g     | area:"山西省,大同市,梅新区"             |
 * -------------------------------------------------------
 *
 */

public class GuidGenerateFunction extends KeyedProcessFunction<String, EventBean, EventBean> {

    Connection conn;
    PreparedStatement preparedStatement;

    org.apache.hadoop.hbase.client.Connection hbaseConn;

    Table deviceAccountBindTable;
    Table deviceTmpGuidTable;
    Table deviceTmpMaxidTable;

    MapStateDescriptor<String, Tuple2<Long, Long>> accountIdStateDescriptor;
    MapStateDescriptor<String, Tuple3<String, Long, Long>> deviceAccountIdStateDescriptor;
    MapStateDescriptor<String,Tuple2<Long,Long>> deviceTmpIdStateDescriptor;

    MapState<String, Tuple2<Long, Long>> accountIdState;
    MapState<String, Tuple3<String, Long, Long>> deviceAccountIdState;
    MapState<String,Tuple2<Long, Long>> deviceTmpIdState;


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
        deviceTmpMaxidTable = hbaseConn.getTable(TableName.valueOf("device_tmp_maxid"));


        // 构造几个用于查询缓存的 state (单值型，list，map，aggreate)
        accountIdStateDescriptor = new MapStateDescriptor<String, Tuple2<Long, Long>>(
                "account-id",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        // 账号,user_id,注册时间
        deviceAccountIdStateDescriptor = new MapStateDescriptor<String, Tuple3<String, Long, Long>>(
                "device-account-id",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple3<String, Long, Long>>() {
                }));

        // 设备id,临时guid,首访时间
        deviceTmpIdStateDescriptor = new MapStateDescriptor<String,Tuple2<Long,Long>>(
                "device-account-id",
                TypeInformation.of(String.class),
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                })
        );

        accountIdState = getRuntimeContext().getMapState(accountIdStateDescriptor);
        deviceAccountIdState = getRuntimeContext().getMapState(deviceAccountIdStateDescriptor);
        deviceTmpIdState = getRuntimeContext().getMapState(deviceTmpIdStateDescriptor);
    }

    /**
     * 填充 guid
     * 填充匿名访客首访时间
     * 填充会员注册时间
     */
    @Override
    public void processElement(EventBean bean, KeyedProcessFunction<String, EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

        // 一、如果bean中有account
        if (StringUtils.isNotBlank(bean.getAccount())) {
            // 先在state中查找是否已有现成的结果
            Tuple2<Long, Long> accountId = accountIdState.get(bean.getAccount());
            if (accountId != null) {
                // 将结果设置到数据bean中
                bean.setGuid(accountId.f0);
                bean.setRegisterTime(accountId.f1);

                // 去hbase中，更新本账号的设备绑定权重（本账号增加，其他账号衰减）
                GuidUtils.deviceAccountBindWeightHandle(bean.getAccount(),bean.getDeviceid(),deviceAccountBindTable,accountId.f0,accountId.f1);


            }else{

                // 从mysql中查询该账号对应的userId和registerTime
                Tuple2<Long, Long> userIdAndRegisterTime = GuidUtils.getUserIdFromMysql(preparedStatement, bean.getAccount());

                // 将结果设置到数据 bean中
                bean.setGuid(userIdAndRegisterTime.f0);
                bean.setRegisterTime(userIdAndRegisterTime.f1);

                // 并将该账号对应的user_id及注册时间信息，存入flink state中  : account,user_id,注册时间
                accountIdState.put(bean.getAccount(), userIdAndRegisterTime);

                // 去hbase中，更新本账号的设备绑定权重（本账号增加，其他账号衰减）
                GuidUtils.deviceAccountBindWeightHandle(bean.getAccount(),bean.getDeviceid(),deviceAccountBindTable,userIdAndRegisterTime.f0,userIdAndRegisterTime.f1);

            }
        }

        boolean flag = false;
        //二、如果bean中没有account
        if(StringUtils.isBlank(bean.getAccount())){

            // 先从 state缓存中查找该 设备的 绑定账号信息 :   账号,userId,注册时间
            Tuple3<String, Long, Long> info = deviceAccountIdState.get(bean.getDeviceid());
            if(info !=null){
                // 取出数据放入结果
                bean.setGuid(info.f1);
                bean.setRegisterTime(info.f2);

                flag = true;
            }
            // 如果在缓存中没有找到  设备对应到绑定账号信息，
            else{
                // 则去hbase中查找 绑定权重最大的账号信息
                DeviceAccountBindInfo bindAccount = GuidUtils.getMaxBindWeigtAccountInfo(deviceAccountBindTable, bean.getDeviceid());

                // 如果找到了绑定的账号
                if(bindAccount != null) {

                    // 取出数据放入结果
                    bean.setGuid(bindAccount.getUserId());
                    bean.setRegisterTime(bindAccount.getRegisterTime());

                    // 并将查询到的结果信息，放入缓存
                    deviceAccountIdState.put(bean.getDeviceid(), Tuple3.of(bindAccount.getAccount(), bindAccount.getUserId(), bindAccount.getRegisterTime()));

                    flag =true;
                }

            }
        }

        // 三、如果 bean中没有account，且在设备账号绑定表中也不存在
        if(StringUtils.isBlank(bean.getAccount())  && !flag){
            // 先从缓存中查找匿名设备的临时guid
            Tuple2<Long, Long> tmpIdInfo = deviceTmpIdState.get(bean.getDeviceid());
            if(tmpIdInfo != null) {
                // 取出数据，放入结果
                bean.setGuid(tmpIdInfo.f0);
                bean.setFirstAccessTime(tmpIdInfo.f1);

                flag = true;
            }
            // 如果缓存中没有找到，则去hbase查找临时guid
            else {
                Tuple2<Long, Long> deviceTmpIdFromHbase = GuidUtils.getDeviceTmpIdFromHbase(deviceTmpGuidTable, bean.getDeviceid());
                if(deviceTmpIdFromHbase != null ) {
                    // 取出数据，放入结果
                    bean.setGuid(deviceTmpIdFromHbase.f0);
                    bean.setFirstAccessTime(deviceTmpIdFromHbase.f1);


                    // 将查询到的结果，存入flink state 中  ： 设备id,临时guid
                    deviceTmpIdState.put(bean.getDeviceid(),deviceTmpIdFromHbase);

                    flag = true;

                }
            }

        }

        // 四、如果 bean中没有account，且在设备账号绑定表中也不存在，且在匿名设备id绑定表也不存在
        if(StringUtils.isBlank(bean.getAccount()) && !flag ) {

            // 请求guid计数器自增得到一个新的临时guid
            Long newTmpId = GuidUtils.getNewTmpId(deviceTmpMaxidTable);

            // 取出数据，放入结果
            bean.setGuid(newTmpId);
            bean.setFirstAccessTime(bean.getTimestamp());


            // 要将递增得到的新临时id，放入  设备-临时id  绑定表
            GuidUtils.addDeviceTmpIdToHbase(deviceTmpGuidTable,bean.getDeviceid(),newTmpId,bean.getTimestamp());


            // 将查询到的结果，存入flink state 中
            deviceTmpIdState.put(bean.getDeviceid(),Tuple2.of(newTmpId,bean.getTimestamp()));

        }


        // 输出结果
        out.collect(bean);

    }


    @Override
    public void close() throws Exception {

        conn.close();
        deviceAccountBindTable.close();
        deviceTmpGuidTable.close();
        deviceTmpMaxidTable.close();

        hbaseConn.close();

    }
}
