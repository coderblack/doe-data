CREATE TABLE dwd.app_log_detail (
     guid                   BIGINT                ,
     eventid                varchar(20)           ,
     releasechannel         String                ,
     account                String                ,
     appid                  String                ,
     appversion             String                ,
     carrier                String                ,
     deviceid               String                ,
     devicetype             String                ,
     ip                     String                ,
     latitude               double                ,
     longitude              double                ,
     nettype                String                ,
     osname                 String                ,
     osversion              String                ,
     resolution             String                ,
     sessionid              String                ,
     `timestamp`            BIGINT                ,
     registerTime           BIGINT                ,
     firstAccessTime        BIGINT                ,
     isNew                  int                   ,
     geoHashCode            String                ,
     province               String                ,
     city                   String                ,
     region                 String                ,
     propsJson              String                ,
     dw_date                date
)
    DUPLICATE KEY(`guid`, `eventid`)
PARTITION BY RANGE(`dw_date`)
(
   PARTITION p20220809 VALUES LESS THAN ("2022-08-10"),
   PARTITION p20220810 VALUES LESS THAN ("2022-08-11"),
   PARTITION p20220811 VALUES LESS THAN ("2022-08-12")
)
DISTRIBUTED BY HASH(`guid`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
