package cn.doitedu.rtdw.etl.pojo;

/**
 * Copyright 2022 bejson.com
 */

import lombok.*;

import java.util.Map;


@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class EventBean {

    private String account;
    private String appid;
    private String appversion;
    private String carrier;
    private String deviceid;
    private String devicetype;
    private String eventid;
    private String ip;
    private double latitude;
    private double longitude;
    private String nettype;
    private String osname;
    private String osversion;
    private Map<String,String> properties;
    private String releasechannel;
    private String resolution;
    private String sessionid;
    private long timestamp;
    private long guid;
    // 如果是注册用户，则这里表示注册的时间
    private long registerTime;
    // 如果是非注册用户，则这里表示首次到访时间
    private long firstAccessTime;

    // 新老访客属性
    private int isNew;

    // geohash码
    private String geoHashCode;

    // 省市区维度字段
    private String province;
    private String city;
    private String region;

    // properties的json格式字段
    // 本字段，是为了方便将处理后的明细日志数据写入doris
    // doris并不支持Map类型
    private String propsJson;

}