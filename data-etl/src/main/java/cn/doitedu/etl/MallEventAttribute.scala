package cn.doitedu.etl

import cn.doitedu.utils.EventAttrUtil
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object MallEventAttribute {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("商城app用户事件归因主题事实表计算任务")
      .enableHiveSupport()
      .master("local")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.hive.convertMetastoreParquet","false")
      .config("spark.sql.hive.convertMetastoreOrc","false")
      .getOrCreate()

    import spark.implicits._



    val df = spark.sql(
      """
        |
        |with tmp as (
        |select
        |   guid
        |   ,event_id
        |   ,event_time
        |   ,if(event_id = 'e1',1,0) as flag
        |from test.doitedu_app_funnel_test
        |where dt='2022-07-16'and
        | (
        |    (event_id='e1' and properties['p1']='v1')  OR
        |    (event_id='e2')  OR
        |    (event_id='e3')  OR
        |    (event_id='e4')
        | )
        |)
        |
        |SELECT
        |  guid
        |  ,collect_list(event_id) as event_seq
        |from
        |(
        |SELECT
        |   guid
        |   ,event_id
        |   ,event_time
        |   ,flag
        |   ,sum(flag) over(partition by guid order by event_time rows between unbounded preceding and current row) - flag as flag2
        |from tmp
        |) o
        |group by guid,flag2
        |having array_contains(collect_list(event_id),'e1')
        |
        |""".stripMargin)

    val resultRdd = df.rdd.flatMap(row => {
      val guid = row.getAs[Long]("guid")
      val eventSeq = row.getSeq[String](1).toArray
      // (1,WrappedArray(e3, e2, e3, e4, e2, e1))

      // 创建一个收集结果的list
      val resultList = new ListBuffer[(Long, String, String, Double)]


      // 根据用户的行为序列，用 首次触点归因，计算一次结果，结果是一行
      // (guid,算法,待归因事件,归因权重)
      // (1,首次触点,e3,100%)
      val tupleFirst = EventAttrUtil.firstTouchAttr(eventSeq)
      resultList += ((guid, "首次触点", tupleFirst._1, tupleFirst._2))


      // 根据用户的行为序列，用 末次触点归因，计算一次结果，结果是一行
      // (guid,算法,待归因事件,归因权重)
      // (1,末次触点,e2,100%)
      val tupleLast = EventAttrUtil.lastTouchAttr(eventSeq)
      resultList += ((guid, "末次触点", tupleLast._1, tupleLast._2))


      // 根据用户的行为序列，用 线性归因，计算一次结果，结果是多行
      // (guid,算法,待归因事件,归因权重)
      // (1,线性归因,e3,40%)
      // (1,线性归因,e2,40%)
      // (1,线性归因,e4,20%)
      val linearResultTuples = EventAttrUtil.linearAttr(eventSeq)
      for (tuple <- linearResultTuples) {
        resultList += ((guid, "线性归因", tuple._1, tuple._2))
      }

      resultList

    })


    resultRdd.toDF("guid","attr_method","attr_event","attr_weight")
      .show(100,false)

    spark.close()
  }

}
