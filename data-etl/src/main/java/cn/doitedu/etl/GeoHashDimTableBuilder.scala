package cn.doitedu.etl

import cn.doitedu.utils.Functions.gps2GeoHashcode
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object GeoHashDimTableBuilder {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("geohash码地域维表构建任务")
      .config("spark.sql.shuffle.partitions", 2)
      .enableHiveSupport()
      .getOrCreate()

    // 加载mysql中的原始地理位置信息数据表
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")

    val df = spark.read.jdbc("jdbc:mysql://doitedu:3306/realtimedw", "t_md_areas", props)
    df.createTempView("t")

    spark.udf.register("geo",gps2GeoHashcode)

    spark.sql(
      """
        |insert overwrite table dim.geohash_area
        |select
        | geohash,
        | province,
        | city,
        | region
        |from(
        |select
        | geohash,
        | province,
        | city,
        | region,
        | row_number() over(partition by geohash order by province) as rn
        |from
        |(
        |   SELECT
        |     geo(lv4.BD09_LAT, lv4.BD09_LNG) as geohash,
        |     lv1.AREANAME as province,
        |     lv2.AREANAME as city,
        |     lv3.AREANAME as region
        |   from t lv4
        |     join t lv3 on lv4.`LEVEL`=4 and lv4.bd09_lat is not null and lv4.bd09_lng is not null and lv4.PARENTID = lv3.ID
        |     join t lv2 on lv3.PARENTID = lv2.ID
        |     join t lv1 on lv2.PARENTID = lv1.ID
        |) o1
        |)o2
        |
        |where rn=1
        |
        |""".stripMargin)

    // res.write.format("hive").mode(SaveMode.Append).saveAsTable("dim.geohash_area")

    spark.close()

  }
}
