package cn.doitedu.rtdw.etl.functions;

import cn.doitedu.rtdw.etl.pojo.EventBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class GeoHashAreaQueryFunction extends KeyedProcessFunction<String, EventBean, EventBean> {
    Connection hbaseConn;
    Table geoTable;

    @Override
    public void open(Configuration parameters) throws Exception {

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "doitedu:2181");
        hbaseConn = ConnectionFactory.createConnection(conf);
        geoTable = hbaseConn.getTable(TableName.valueOf("dim_geo_area"));


    }

    @Override
    public void processElement(EventBean bean, KeyedProcessFunction<String, EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

        String geoHashCode = bean.getGeoHashCode();
        String province = "未知";
        String city = "未知";
        String region = "未知";

        boolean flag = false;

        if (StringUtils.isNotBlank(geoHashCode)) {
            // hbase中地理位置信息的数据结构：  geohash码  ->   f:q -> "江西省,南昌市,鄱阳湖区"
            Result result = geoTable.get(new Get(Bytes.toBytes(geoHashCode)));
            byte[] value = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));

            if (value != null) {
                String[] split = new String(value).split(",");
                if (split.length == 3) {
                    province = split[0];
                    city = split[1];
                    region = split[2];

                    flag = true;

                }
            }
        }

        bean.setProvince(province);
        bean.setCity(city);
        bean.setRegion(region);


        out.collect(bean);

        // 如果地理位置解析失败，则将本条数据的gps座标，输出到测流
        if(!flag ) {
            ctx.output(new OutputTag<String>("unknown_gps", TypeInformation.of(String.class)), bean.getLatitude() + "," + bean.getLongitude());
        }
    }


}
