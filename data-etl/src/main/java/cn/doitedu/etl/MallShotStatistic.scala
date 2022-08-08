package cn.doitedu.etl

import org.apache.spark.sql.SparkSession

object MallShotStatistic {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("打靶")
      .enableHiveSupport()
      .config("hive.metastore.uris","thrift://doitedu:9083")
      .getOrCreate()

    val dt = args(0)

    spark.sql(
      s"""
        |
        |insert into table default.dol_test2 partition(dt='${dt}')
        |select
        |  gender,
        |  count(1) as shot_cnt,
        |  avg(score) as avg_score,
        |  max(score) as max_score,
        |  min(score) as min_score
        |from default.dol_test1
        |group by gender
        |
        |""".stripMargin)


    spark.close()
  }
}
