package cn.doitedu.profile

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.{RandomUtils, StringUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties


object LoadUserIds {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .config("spark.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.textFile("hdfs://doitedu:8020/userid/")
    val df = ds.rdd.map(s => {
      val nObject = JSON.parseObject(s)
      val account = nObject.getString("account")

      val day = StringUtils.leftPad(RandomUtils.nextInt(1, 11) + "", 2, "0")
      val hour = StringUtils.leftPad(RandomUtils.nextInt(1, 13) + "", 2, "0")
      val minute = StringUtils.leftPad(RandomUtils.nextInt(1, 60) + "", 2, "0")
      val second = StringUtils.leftPad(RandomUtils.nextInt(1, 60) + "", 2, "0")

      (account,s"2022-08-${day} ${hour}:${minute}:${second}")
    }).toDF("account","register_time")

    df.distinct().createTempView("tv")

    val res = spark.sql("select  row_number() over(order by account) as id,account,register_time from tv ")

    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")
    res.write.mode(SaveMode.Append).jdbc("jdbc:mysql://doitedu:3306/rtmk","ums_member",props)


    spark.close()



  }

}
