package cn.doitedu.etl

import cn.doitedu.utils.{PageContributeUtil, TreeNode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

// |guid|session_id|page_id|referPage   |event_time   |session_endtime|
case class PageBean(
                     guid: Long,
                     session_id: String,
                     page_id: String,
                     referPage: String,
                     event_time: Long,
                     session_endtime: Long
                   )

case class ResultBean(dt:String ,guid: Long, session_id: String, page_id: String, pv_cnt: Int, page_staytime: Long, pv_contribute_d: Int, pv_contribute_w: Int)

object AppTrafficFactTableBuilder {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("商城app流量分析-星型模型-事实表计算")
      .enableHiveSupport()
      .config("spark.sql.shuffle.partitions","2")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()


    val df = spark.sql(
      """
        |select
        |  o1.guid
        |  ,o1.session_id
        |  ,o1.page_id
        |  ,o1.referPage
        |  ,o1.event_time
        |  ,o2.session_endtime
        |
        |from
        |(
        |select
        |  guid
        |  ,session_id
        |  ,properties['pageId'] as page_id
        |  ,properties['referPage'] as referPage
        |  ,event_time
        |from dwd.doitedu_mall_app_events
        |where dt='2022-07-16' and event_id = 'PAGELOAD'
        |) o1
        |left join
        |(select session_id,session_endtime from dws.doitedu_mall_app_aggr_session where dt='2022-07-16') o2
        |on o1.session_id=o2.session_id
        |
        |
        |""".stripMargin)


    /**
     * +----+----------+-------+------------+-------------+---------------+
     * |guid|session_id|page_id|referPage   |event_time   |session_endtime|
     * +----+----------+-------+------------+-------------+---------------+
     * |1003|ACMWTTERBN|index   |null        |1658043285033|1658043300795  |
     * |1003|ACMWTTERBN|page023|index       |1658043297269|1658043300795  |
     * |1003|ACMWTTERBN|page004|page023     |1658043299036|1658043300795  |
     * |1003|ACMWTTERBN|page039|page004     |1658043299794|1658043300795  |
     *
     */

    import spark.implicits._
    val rdd = df.as[PageBean].rdd

    /**
     * PageBean(1846,ZYBFEFJFQW,index,null,1658043122712,1658043592901)
     * PageBean(1846,ZYBFEFJFQW,page043,index,1658043581146,1658043592901)
     * PageBean(1846,ZYBFEFJFQW,page001,index,1658043582115,1658043592901)
     * PageBean(1846,ZYBFEFJFQW,page023,index,1658043583601,1658043592901)

     * PageBean(1846,ZYBFEFJFQW,page045,page023,1658043589124,1658043592901)
     */


    val grouped = rdd.groupBy(pageBean => (pageBean.guid, pageBean.session_id))
    /*
       分组key(1846,ZYBFEFJFQW),  Iterable[PageBean,PageBean,PageBean,PageBean]
       分组key(2835,YEWISJSDGJ),  Iterable[PageBean,PageBean,PageBean,PageBean]
     */

    val result: RDD[ResultBean] = grouped.flatMap(tp => {
      val iter: Iterable[PageBean] = tp._2 // 这个iter就代表着一组数据： 相同人的相同会话中的所有页面访问记录


      val pageloadEventList = iter.toList.sortBy(bean => bean.event_time)

      /**
       * 计算本次会话中的，每个页面的pv
       */

      val pagePvs: Map[String, Int] = pageloadEventList.groupBy(bean => bean.page_id).map(tp => (tp._1, tp._2.size))

      /**
       * 计算本次会话中的，每个页面的停留时长
       */
      val pageStaytimeMap: mutable.HashMap[String, Long] = mutable.HashMap.empty

      for (i <- pageloadEventList.indices) {

        var staytime: Long = 0

        if (i != pageloadEventList.size - 1) {
          // 如果不是最后一个页面访问记录，则它的停留时长 = 下一个页面的事件时间 - 自己的事件时间
          staytime = pageloadEventList(i + 1).event_time - pageloadEventList(i).event_time
        } else {
          // 如果是最后一个页面访问记录，则它的停留时长 = 整个会话的结束时间 - 自己的事件时间
          staytime = pageloadEventList(i).session_endtime - pageloadEventList(i).event_time
        }

        // 每算完一个页面的停留时长，就放入结果hashmap中存储(如果之前已经存在这个页面，则把时间累加
        pageStaytimeMap += ((pageloadEventList(i).page_id, pageStaytimeMap.getOrElse(pageloadEventList(i).page_id, 0L) + staytime))

      }

      /**
       * 计算贡献量
       */
      // 首先，将这个会话中的所有页面访问记录，构建成一棵树
      var node: TreeNode = null
      for (pageBean <- pageloadEventList) {
        if (node == null) {
          node = TreeNode(pageBean.page_id, ListBuffer.empty)
        } else {
          PageContributeUtil.findAndAppend(node, pageBean.page_id, if (pageBean.referPage == null) "直接访问" else pageBean.referPage)
        }
      }

      // 计算总贡献量
      /*

      a -- |
           |---b---|--- d
                   |--- e
           |---c---|
                   |---a--|
                          |---x

       */

      val lst = ListBuffer.empty[(String, Int)]
      PageContributeUtil.calcWholeContributePv(node, lst) // List[(a,6),(b,2),(c,2),(a,1),(x,0)]
      val wholeContributeMap: Map[String, Int] =
        lst.groupBy(tp => tp._1) // (a,List((a,6),(a,1)))
          .map(tp => (tp._1, tp._2.map(tp => tp._2).sum))


      val lst2 = ListBuffer.empty[(String, Int)]
      PageContributeUtil.calcDirectContributePv(node, lst2) // List[(a,2),(b,2),(c,2),(a,1),(x,0)]
      val directContributeMap: Map[String, Int] =
        lst2.groupBy(tp => tp._1) // (a,List((a,2),(a,1)))
          .map(tp => (tp._1, tp._2.map(tp => tp._2).sum))


      /**
       * pagePvs,
       * pageStaytimeMap,
       * directContributeMap,
       * wholeContributeMap,
       * 这4部分结果，按照相同页面整合拼接 得到最终要的结果：
       * guid,session_id,   page_id ?,  pv ?, pageStaytime ?  , 直接贡献量?   总贡献量 ?
       */

      for (kv <- pagePvs) yield {
        ResultBean("2022-07-16",tp._1._1, tp._1._2, kv._1, kv._2, pageStaytimeMap.getOrElse(kv._1, 0L), directContributeMap.getOrElse(kv._1, 0), wholeContributeMap.getOrElse(kv._1, 0))
      }


    })


    // 输出结果到hive的目标表中
    val resultDF = result.toDF()
    resultDF.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .partitionBy("dt")
      .saveAsTable("dws.doitedu_mall_app_traffic_aggr_usp")

    spark.close()


  }
}
