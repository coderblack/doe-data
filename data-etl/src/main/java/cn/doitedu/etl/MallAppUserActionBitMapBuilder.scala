package cn.doitedu.etl

import org.apache.spark.sql.SparkSession
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/7/26
 * @Desc:
 * 将数仓中已经存在的用户活跃情况，生成一个活跃bitmap记录表的初始状态
 * -- 从 活跃区间记录表 来处理
 *
 * --先得到每一个活跃区间的bitmap
 * g01,2022-07-01,2022-07-10   ->  bm
 * g01,2022-07-15,2022-07-17   ->  bm
 * g02,2022-07-18,9999-12-31
 *
 * --然后，将相同用户分组，把他的所有 bm 收集到一个数组中，然后进行合并，得到最终的bm
 *
 * -- 目标结果
 * g01,2022-07-01, [00000000000000000000000001010000100000000]
 * g02,2022-07-18, [00000000000000000000000101000000000000000]
 * */
object MallAppUserActionBitMapBuilder {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.shuffle.partitions", "2")
      .appName("用户活跃bitmap模型表初始构建")
      .getOrCreate()

    val genBitMap = (start: Int, end: Int) => {
      val bm = RoaringBitmap.bitmapOf(start.to(end).toArray: _*)
      val baout = new ByteArrayOutputStream()
      val dout = new DataOutputStream(baout)
      bm.serialize(dout)

      baout.toByteArray
    }

    val orMergeBitMap = (bmArr: Array[Array[Byte]]) => {

      val bm = RoaringBitmap.bitmapOf()
      for (bmBytes <- bmArr) {

        // 反序列化本次遍历到的bitmap的序列化字节
        val bmTmp = RoaringBitmap.bitmapOf();
        val bin = new ByteArrayInputStream(bmBytes)
        val din = new DataInputStream(bin)

        bmTmp.deserialize(din)

        // 合并
        bm.or(bmTmp)
      }

      // 将合并好的bitmap，序列化成字节返回
      val baout = new ByteArrayOutputStream()
      val dout = new DataOutputStream(baout)
      bm.serialize(dout)

      baout.toByteArray
    }


    spark.udf.register("gen_bitmap", genBitMap)
    spark.udf.register("or_merge_bitmap", orMergeBitMap)

    spark.sql(
      """
        |insert into table dws.doitedu_mall_app_user_active_bm partition(dt='2022-07-16')
        |select
        |  o1.guid
        |  ,o2.first_login_dt
        |  ,o1.bm
        |from
        |(
        |    select
        |       guid
        |       ,or_merge_bitmap( collect_list(bm) ) as bm
        |    from (
        |        select
        |          guid
        |          ,gen_bitmap(datediff(range_start_dt,'2000-01-01'),datediff(range_end_dt,'2000-01-01')) as bm
        |        from dws.doitedu_mall_app_user_actrang
        |        where dt='2022-07-16'
        |     ) t
        |    group by guid
        |) o1
        |
        |join
        |(
        |   select
        |      guid
        |      ,min(range_start_dt) as first_login_dt
        |   from dws.doitedu_mall_app_user_actrang
        |   where dt='2022-07-16'
        |   group by guid
        |) o2
        |on o1.guid = o2.guid
        |
        |
        |""".stripMargin)


    spark.close()
  }

}
