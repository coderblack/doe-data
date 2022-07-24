package cn.doitedu.etl

import cn.doitedu.utils.Functions
import org.apache.spark.sql.{SaveMode, SparkSession}

object MallApplogOds2DwdStep3 {

  def main(args: Array[String]): Unit = {
    var dt = "2022-07-16"

    if(args.length>0){
      dt = args(0)
    }

    val spark = SparkSession.builder()
      .appName("MallApplogOds2DwdStep3")
      .config("spark.sql.shuffle.partitions","1")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()



    spark.udf.register("geo",Functions.gps2GeoHashcode)

    val joined = spark.sql(
      """
        |select
        |  a.*,
        |  b.province,
        |  b.city,
        |  b.region
        |from
        |  tmp.app_log_ods2dwd_step2 a  -- 日志表
        |left join
        |  dim.geohash_area b  -- 地域维表
        |on geo(a.latitude,a.longitude)=b.geohash
        |
        |""".stripMargin)

    joined.createTempView("joined")

    // 主输出：就是把关联处理后的日志输出，插入到dwd日志明细表中区
    spark.sql(
      s"""
        |insert overwrite table dwd.doitedu_mall_app_events partition(dt='${dt}')
        |select * from joined
        |
        |""".stripMargin)


    // 从查询结果中挑出关联地域信息失败的gps座标，进行侧输出
    // 以便于后续可以用异步任务去对这些gps座标请求高德来得到地域信息
    spark.sql(
      """
        |select
        |  concat_ws(',',latitude,longitude)
        |from joined
        |where province is null
        |group by latitude,longitude
        |
        |""".stripMargin)
      .write.mode(SaveMode.Overwrite).text(s"hdfs://doitedu:8020/unknown-gps/${dt}")

    spark.close()
  }

}
