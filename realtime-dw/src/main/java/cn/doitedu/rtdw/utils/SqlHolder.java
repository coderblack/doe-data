package cn.doitedu.rtdw.utils;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/4
 **/
public class SqlHolder {

    public static final String DORIS_DETAIL_SINK_DDL
            = "CREATE TABLE doris_appdetail_sink (\n" +
            "  guid                   BIGINT                ,\n" +
            "  eventid                String                ,\n" +
            "  releasechannel         String                ,\n" +
            "  account                String                ,\n" +
            "  appid                  String                ,\n" +
            "  appversion             String                ,\n" +
            "  carrier                String                ,\n" +
            "  deviceid               String                ,\n" +
            "  devicetype             String                ,\n" +
            "  ip                     String                ,\n" +
            "  latitude               double                ,\n" +
            "  longitude              double                ,\n" +
            "  nettype                String                ,\n" +
            "  osname                 String                ,\n" +
            "  osversion              String                ,\n" +
            "  resolution             String                ,\n" +
            "  sessionid              String                ,\n" +
            "  `timestamp`            BIGINT                ,\n" +
            "  registerTime           BIGINT                ,\n" +
            "  firstAccessTime        BIGINT                ,\n" +
            "  isNew                  int                   ,\n" +
            "  geoHashCode            String                ,\n" +
            "  province               String                ,\n" +
            "  city                   String                ,\n" +
            "  region                 String                ,\n" +
            "  propsJson              String                ,\n" +
            "  dw_date                STRING                 \n" +
            ")                                               \n" +
            "    WITH (                                      \n" +
            "      'connector' = 'doris',                    \n" +
            "      'fenodes' = 'doitedu:8030',               \n" +
            "      'table.identifier' = 'dwd.app_log_detail',\n" +
            "      'username' = 'root',                      \n" +
            "      'password' = ''  ,                        \n" +
            "      'sink.label-prefix' = 'mall_app_events'   \n" +
            ")";


    public static final String DORIS_DETAIL_SINK_INSERT
            = "INSERT INTO doris_appdetail_sink      \n" +
            "SELECT              \n" +
            "  guid             ,\n" +
            "  eventid          ,\n" +
            "  releasechannel   ,\n" +
            "  account          ,\n" +
            "  appid            ,\n" +
            "  appversion       ,\n" +
            "  carrier          ,\n" +
            "  deviceid         ,\n" +
            "  devicetype       ,\n" +
            "  ip               ,\n" +
            "  latitude         ,\n" +
            "  longitude        ,\n" +
            "  nettype          ,\n" +
            "  osname           ,\n" +
            "  osversion        ,\n" +
            "  resolution       ,\n" +
            "  sessionid        ,\n" +
            "  `timestamp`      ,\n" +
            "  registerTime     ,\n" +
            "  firstAccessTime  ,\n" +
            "  isNew            ,\n" +
            "  geoHashCode      ,\n" +
            "  province         ,\n" +
            "  city             ,\n" +
            "  region           ,\n" +
            "  propsJson        ,\n" +
            "  dw_date           \n" +
            "FROM logdetail"   ;

    public static final String KAFKA_DETAIL_SINK_DDL
            ="CREATE TABLE kafka_dwd_sink (                  \n" +
            "  eventid                String                ,\n" +
            "  guid                   BIGINT                ,\n" +
            "  releasechannel         String                ,\n" +
            "  account                String                ,\n" +
            "  appid                  String                ,\n" +
            "  appversion             String                ,\n" +
            "  carrier                String                ,\n" +
            "  deviceid               String                ,\n" +
            "  devicetype             String                ,\n" +
            "  ip                     String                ,\n" +
            "  latitude               double                ,\n" +
            "  longitude              double                ,\n" +
            "  nettype                String                ,\n" +
            "  osname                 String                ,\n" +
            "  osversion              String                ,\n" +
            "  resolution             String                ,\n" +
            "  sessionid              String                ,\n" +
            "  `timestamp`            BIGINT                ,\n" +
            "  registerTime           BIGINT                ,\n" +
            "  firstAccessTime        BIGINT                ,\n" +
            "  isNew                  int                   ,\n" +
            "  geoHashCode            String                ,\n" +
            "  province               String                ,\n" +
            "  city                   String                ,\n" +
            "  region                 String                ,\n" +
            "  propsJson              String                ,\n" +
            "  dw_date                STRING                 \n" +
            ") WITH (                                        \n" +
            "  'connector' = 'kafka',                        \n" +
            "  'topic' = 'dwd-applog-detail',                \n" +
            "  'properties.bootstrap.servers' = 'doitedu:9092',    \n" +
            "  'properties.group.id' = 'dwdsink',            \n" +
            "  'scan.startup.mode' = 'latest-offset',        \n" +
            "  'format' = 'json'                             \n" +
            ")";


    public static final String KAFKA_DETAIL_SINK_DML
            = "INSERT INTO kafka_dwd_sink      \n" +
                    "SELECT              \n" +
                    "  eventid          ,\n" +
                    "  guid             ,\n" +
                    "  releasechannel   ,\n" +
                    "  account          ,\n" +
                    "  appid            ,\n" +
                    "  appversion       ,\n" +
                    "  carrier          ,\n" +
                    "  deviceid         ,\n" +
                    "  devicetype       ,\n" +
                    "  ip               ,\n" +
                    "  latitude         ,\n" +
                    "  longitude        ,\n" +
                    "  nettype          ,\n" +
                    "  osname           ,\n" +
                    "  osversion        ,\n" +
                    "  resolution       ,\n" +
                    "  sessionid        ,\n" +
                    "  `timestamp`      ,\n" +
                    "  registerTime     ,\n" +
                    "  firstAccessTime  ,\n" +
                    "  isNew            ,\n" +
                    "  geoHashCode      ,\n" +
                    "  province         ,\n" +
                    "  city             ,\n" +
                    "  region           ,\n" +
                    "  propsJson         ,\n" +
                    "  dw_date           \n" +
                    "FROM logdetail"   ;



    public static final String TRAFFIC_AGG_USER_SESSION =
            "create view traffic_agg_user_session  as     select                                                               \n" +
                    "  guid                                                                                \n" +
                    "  ,splitSessionId                                                                     \n" +
                    "  ,province                                                                           \n" +
                    "  ,city                                                                               \n" +
                    "  ,region                                                                             \n" +
                    "  ,deviceType                                                                         \n" +
                    "  ,isNew                                                                              \n" +
                    "  ,releaseChannel                                                                     \n" +
                    "  ,max(ts)-min(ts) as sessionTimeLong                                                 \n" +
                    "  ,sum(if(eventId='pageload',1,0)) as sessionPv                                       \n" +
                    "from traffic                                                                          \n" +
                    "group by guid,splitSessionId,province,city,region,deviceType,isNew,releaseChannel   ";

    public static final  String TRAFFIC_DIM_ANA_01 =
                    "   select                                " +
                    "     province                           " +
                    "     ,city                               " +
                    "     ,region                             " +
                    "     ,deviceType                         " +
                    "     ,releaseChannel                     " +
                    "     ,isNew                              " +
                    "     ,count(distinct guid) as uv         " +
                    "     ,sum(sessionPv) as pv               " +
                    "     ,sum(sessionTimeLong) as timeLong   " +
                    "     ,count(1) as sessionCout            " +
                    "   from traffic_agg_user_session         " +
                    "   group by grouping sets(               " +
                    "    ()                                   " +
                    "    ,(releaseChannel)                    " +
                    "    ,(releaseChannel,isNew)              " +
                    "    ,(deviceType)                        " +
                    "    ,(province,city,region)              " +
                    "   )                                     " ;


    public static final String Page_STAT_AGG =
            "        create temporary view pageStatistic                      " +
            "         as                                                      " +
            "        select                                                   " +
            "           pageId,                                               " +
            "           sum(pageTimeLong) as pageTimeLong,                    " +
            "           count(distinct splitSessionId) as sessionCnt,         " +
            "           count(1)  as pagePv                                   " +
            "           count(distinct guid)  as pageUv                       " +
            "        from (                                                   " +
            "            select                                               " +
            "              pageId,pageLoadTime,splitSessionId,guid,           " +
            "              max(ts) - min(ts) as pageTimeLong                  " +
            "            from traffic                                         " +
            "            where pageId is not null                             " +
            "            group by pageId,pageLoadTime,splitSessionId,guid     " +
            "        )tmp                                                     " +
            "        group by pageId                                          ";

    /**
     * 流量主题多维分析报表2：
     *    按每小时段进行累计窗口统计各维度组合下的pv、uv、访问时长、会话数等
     */
    public static final String TRAFFIC_DIM_ANA_02
            = "SELECT                                                                                                          " +
            "   window_start,                                                                                                " +
            "   window_end ,                                                                                                 " +
            "   releaseChannel,                                                                                              " +
            "   isNew,                                                                                                       " +
            "   deviceType,                                                                                                  " +
            "   province,                                                                                                    " +
            "   city,                                                                                                        " +
            "   region,                                                                                                      " +
            "   count(distinct guid) as uv ,                                                                                 " +
            "   sum(sessionPv) as pv,                                                                                        " +
            "   sum(sessionTimeLong) as accTimeLong,                                                                         " +
            "   count(1) as sessionCnt                                                                                       " +
            "FROM TABLE(                                                                                                     " +
            "  CUMULATE(TABLE traffic_agg_user_session,DESCRIPTOR(rt),INTERVAL '10' MINUTES, INTERVAL '60' MINUTES)          " +
            ")                                                                                                               " +
            "GROUP BY grouping sets(                                                                                         " +
            "  (window_start,window_end,releaseChannel),                                                                     " +
            "  (window_start,window_end,releaseChannel,isNew),                                                               " +
            "  (window_start,window_end,deviceType),                                                                         " +
            "  (window_start,window_end,province,city,region)                                                                " +
            ")                                                                                                               ";


    public static final String NEW_RETENTION_ANA
            = "select                                                                                                                                                                                       " +
            "   date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd') AS CALC_DATE                                                                                                                                    " +
            "  ,releaseChannle                                                                                                                                                                              " +
            "  ,deviceType                                                                                                                                                                                  " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -1, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_1day        " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -2, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_2day        " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -3, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_3day        " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -4, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_4day        " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -5, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_5day        " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -6, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_6day        " +
            "  ,sum(if( date_format(from_unixtime(nvl(registerTime,firstAccessTime)/1000),'yyyy-MM-dd') = date_format( timestampadd(day, -7, CURRENT_TIMESTAMP ), 'yyyy-MM-dd' ) ,1,0) ) as ret_7day        " +
            "from traffic                                                                                                                                                                                   " +
            "group by releaseChannle,deviceType                                                                                                                                                             ";


}
