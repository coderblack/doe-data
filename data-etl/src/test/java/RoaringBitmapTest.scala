import cn.hutool.core.date.{DateUnit, DateUtil}
import jodd.datetime.TimeZoneUtil
import org.apache.commons.lang3.time.DateUtils
import org.roaringbitmap.RoaringBitmap

import java.time.ZonedDateTime
import java.util.{Calendar, TimeZone}

object RoaringBitmapTest {


  def main(args: Array[String]): Unit = {

    // 创建一个bitmap

    val bitmap: RoaringBitmap = RoaringBitmap.bitmapOf(1, 3, 4,5, 6, 7)
    bitmap.add(9)

    println(bitmap.contains(3,6))
    println(bitmap.contains(9))

    // 我们要记录 是：  "2022-07-01"  , "2022-07-03" , "2022-07-04" , "2022-07-05"
    // bitmap中的角标：  日期- 固定起始日(“2010-01-01”)
    val initDate = DateUtils.parseDate("2010-01-01", "yyyy-MM-dd")
    val date1= DateUtils.parseDate("2022-07-01", "yyyy-MM-dd")
    val date3 = DateUtils.parseDate("2022-07-03", "yyyy-MM-dd")
    val date4 = DateUtils.parseDate("2022-07-04", "yyyy-MM-dd")
    val date5 = DateUtils.parseDate("2022-07-05", "yyyy-MM-dd")
    val date6 = DateUtils.parseDate("2022-07-06", "yyyy-MM-dd")


    /**
     * 构造活跃记录
     */
    val bitmap2 = RoaringBitmap.bitmapOf(DateUtil.between(date1, initDate, DateUnit.DAY).toInt)
    bitmap2.add(DateUtil.between(date3, initDate, DateUnit.DAY).toInt)
    bitmap2.add(DateUtil.between(date4, initDate, DateUnit.DAY).toInt)
    bitmap2.add(DateUtil.between(date5, initDate, DateUnit.DAY).toInt)



    // 查询 2022-07-03 是否有活跃
    println(bitmap2.contains(DateUtil.between(date3, initDate, DateUnit.DAY).toInt))

    // 查询 2022-07-03 ~  2022-07-05 是否连续活跃
    val res = bitmap2.contains(DateUtil.between(date3, initDate, DateUnit.DAY).toInt, DateUtil.between(date5, initDate, DateUnit.DAY).toInt)
    println(res)



  }


}
