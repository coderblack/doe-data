package cn.doitedu.profile.`export`

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
      Map("guid"->1,"tg01" -> 5, "tg02" -> 10, "tg03" -> "高富帅", "tg04" -> List("高端家具","汽车保养", "小罐咖啡")),
      Map("guid"->2,"tg01" -> 4, "tg02" -> 20, "tg03" -> "白富美", "tg04" -> List("兰蔻精华液","特仑苏牛奶", "香奈儿","高尔夫球场","高尔夫运动服饰","汽车内饰"),"tg05"->"女"),
      Map("guid"->3,"tg01" -> 3, "tg02" -> 15, "tg03" -> "全职妈妈", "tg04" -> List("惠氏奶粉牛奶", "宝宝润肤露","运动健身计划")),
      Map("guid"->4,"tg01" -> 3, "tg02" -> 14, "tg03" -> "职场人士", "tg04" -> List("兰蔻小黑瓶", "宝宝润肤露","家用汽车购置攻略"),"tg05"->"男")
    ))

    rdd.saveToEs("doeusers/",Map("es.mapping.id" -> "guid"))

    sc.stop()

  }
}
