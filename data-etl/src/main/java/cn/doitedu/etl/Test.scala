package cn.doitedu.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class DNS(ip:String,domain:String,hour:String)
object Test {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()
    val rdd = sc.textFile("/inpath")
    rdd.map(s => {
      // 192.168.1.2|www.baidu.com|2022-01-12 09:35:40
      val split = s.split("\\|")
      val hour = split(2).split(":")(0)
      ((split(0), split(1),hour),1)
    }).reduceByKey(_+_)
      .map(tp=>(tp._1._1,tp._1._3,tp._1._2,tp._2)) // ip,hour,domain,cnt
      .groupBy(tp=>(tp._1,tp._2))
      .flatMap(tp=>{
        tp._2.toList.sortBy(tp=> -tp._4).slice(0,100)
      }).saveAsTextFile("/outpath")

  }
}
