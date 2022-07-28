package cn.doitedu.etl

import cn.hutool.core.date.DateUtil
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import scala.collection.mutable

object MallUserRetentionRPT_A {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("商城app用户留存分析报表")
      .enableHiveSupport()
      .master("local")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.createDataset(Seq(
      "0,2022-04-01,2022-04-20",
      "0,2022-05-02,2022-07-08",
      "1,2022-07-01,2022-07-10",
      "1,2022-07-15,2022-07-17",
      "2,2022-07-01,2022-07-10",
      "3,2022-07-02,2022-07-04",
      "3,2022-07-12,9999-12-31",
      "4,2022-07-03,2022-07-03",
      "4,2022-07-08,2022-07-10",
      "4,2022-07-25,2022-08-02", // 跨范围
      "5,2022-07-08,2022-07-10",
      "5,2022-08-18,2022-08-20", // 完全超出范围
      "4,2022-08-08,2022-08-14"  // 完全超出范围
    ))

    ds.map(s=>{
      val strings = s.split(",")
      (strings(0).toLong,strings(1),strings(2))
    }).toDF("guid","range_start_dt","range_end_dt")
      .createTempView("doitedu_mall_app_user_actrange")




    // 读源表：   app用户活跃区间记录表
    spark.sql(
      """
        |
        |
        |select
        |   o2.guid
        |   ,o2.range_start_dt
        |   ,if(o2.range_end_dt>'2022-07-31','2022-07-31',o2.range_end_dt) as range_end_dt --对跨范围的区间end做修正
        |   ,o1.first_login_dt
        |from
        |    (
        |       -- 1.先计算出每个用户的真实首登日，并过滤出首登日落在5-7月份的
        |       select
        |          guid,
        |          min(range_start_dt) as first_login_dt
        |       from doitedu_mall_app_user_actrange
        |       group by guid
        |       having  min(range_start_dt) between '2022-05-01'  and '2022-07-31'
        |    ) o1
        |JOIN
        |   -- 2. 通过join ，排除掉不属于 5~7月的新用户
        |   doitedu_mall_app_user_actrange o2
        |ON o1.guid=o2.guid
        |where range_start_dt <= '2022-07-31'  -- 限定 区间 一定要在 统计范围内
        |
        |
        |""".stripMargin).createTempView("tmp")

    /*
           +----+--------------+------------+--------------+
           |guid|range_start_dt|range_end_dt|first_login_dt|
           +----+--------------+------------+--------------+
           |1   |2022-07-01    |2022-07-10  |2022-07-01    |
           |1   |2022-07-15    |2022-07-17  |2022-07-01    |
           |2   |2022-07-01    |2022-07-10  |2022-07-01    |
           |3   |2022-07-02    |2022-07-04  |2022-07-02    |
           |3   |2022-07-12    |2022-07-31  |2022-07-02    |
           |4   |2022-07-03    |2022-07-03  |2022-07-03    |
           |4   |2022-07-08    |2022-07-10  |2022-07-03    |
           |4   |2022-07-25    |2022-07-31  |2022-07-03    |
           |5   |2022-07-08    |2022-07-10  |2022-07-08    |
           +----+--------------+------------+--------------+
     */

    spark.sql(
      """
        |
        |select
        |   guid
        |   ,max(first_login_dt) as first_login_dt
        |   ,collect_list(concat_ws(':',range_start_dt,range_end_dt)) as range_lst
        |from tmp
        |group by guid
        |
        |""".stripMargin).createTempView("tmp2")

    /*
    +----+--------------+---------------------------------------------------------------------+
     |guid|first_login_dt|collect_list(concat_ws(:, range_start_dt, range_end_dt))             |
     +----+--------------+---------------------------------------------------------------------+
     |1   |2022-07-01    |[2022-07-01:2022-07-10, 2022-07-15:2022-07-17]                       |
     |2   |2022-07-01    |[2022-07-01:2022-07-10]                                              |
     |3   |2022-07-02    |[2022-07-02:2022-07-04, 2022-07-12:2022-07-31]                       |
     |4   |2022-07-03    |[2022-07-03:2022-07-03, 2022-07-08:2022-07-10, 2022-07-25:2022-07-31]|
     |5   |2022-07-08    |[2022-07-08:2022-07-10]                                              |
     +----+--------------+---------------------------------------------------------------------+
     */


    /*val rdd: RDD[Row] = spark.sql("select * from tmp2").rdd

    rdd.map(row=>{

      val guid = row.getAs[Long]("guid")
      val first_login_dt = row.getAs[String]("first_login_dt")
      val lst = row.get(2)
      lst


    }).take(2).foreach(println)*/


    /**
     * 自定义一个函数，用来判断一个用户是否属于某种 N日留存
     */
    val retentionJudge = (rangeLst:mutable.ArrayBuffer[String], targetDate:String) => {

      // 遍历用户的每一个活跃区间
      for (range <- rangeLst) {
        val rangeStartAndEnd = range.split(":")
        // 如果该区间包含 要判断的 ”留存日期“ ，则返回1
        if(targetDate >= rangeStartAndEnd(0)  && targetDate <= rangeStartAndEnd(1)) return 1
      }

      // 如果没有任何一个区间包含  要判断的“留存日期”， 则返回0 ： 表示该用户不属于这种 N日留存
      0
    }


    spark.udf.register("retention_judge",retentionJudge)


    // 给每个人，打上 3种留存标记
    spark.sql(
      """
        |
        |select
        |   guid
        |   ,retention_judge(range_lst,date_add(first_login_dt,1))  as r1
        |   ,retention_judge(range_lst,date_add(first_login_dt,3))  as r3
        |   ,retention_judge(range_lst,date_add(first_login_dt,7))  as r7
        |from tmp2
        |
        |""".stripMargin).show(100,false)

    /*spark.sql(
      """
        |select date_add(first_login_dt,1) from tmp2
        |""".stripMargin).show(100,false)*/



    spark.close()










  }
}
