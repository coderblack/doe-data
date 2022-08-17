package cn.doitedu.profile.`export`

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveTags2Es {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "doitedu")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")

    val spark = SparkSession.builder()
      .config(conf)
      .master("local")
      .appName("hive标签数据导入es")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.table("test.user_profile_test").where("dt='2022-08-17'")

    val tagsRdd = df.rdd.map(row => {
      Map("guid" -> row.getAs[Int]("guid"),
        "tg01" -> row.getAs[Int]("tg01"),
        "tg02" -> row.getAs[Int]("tg02"),
        "tg03" -> row.getAs[String]("tg03"),
        "tg04" -> row.getAs[Array[String]]("tg04")
      )
    })


    import org.elasticsearch.spark._
    tagsRdd.saveToEs("doeusers/",Map("es.mapping.id" -> "guid"))


    spark.close()

  }

}
