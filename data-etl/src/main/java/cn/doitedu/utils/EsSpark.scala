package cn.doitedu.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/13
 * @Desc:  spark写入数据到es，简单示例


curl -X GET "localhost:9200/docs/_search?from=0&size=20&pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {
      "tg04": "服装"
    }
  }
}
'

 *
 *
 * */
object EsSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "doitedu")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")
//      .set("es.net.http.auth.user", "elxxxxastic")
//      .set("es.net.http.auth.pass", "xxxx")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(
      Map("id"->1,"tg01" -> 5, "tg02" -> 10, "tg03" -> "高富帅", "tg04" -> List("改革开发", "高档食品")),
      Map("id"->2,"tg01" -> 4, "tg02" -> 20, "tg03" -> "白富美", "tg04" -> List("农村幼儿园", "运动饮料")),
      Map("id"->3,"tg01" -> 3, "tg02" -> 15, "tg03" -> "全职妈妈", "tg04" -> List("城市幼儿园", "运动服装"))
    ))

    rdd.saveToEs("docs/",Map("es.mapping.id" -> "id"))

    sc.stop()

  }
}
