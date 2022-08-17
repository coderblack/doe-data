//package cn.doitedu.profile
//
//import cn.doitedu.utils.PropertiesHolder
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.hbase.client.ConnectionFactory
//import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
//import org.apache.hadoop.hbase.tool.BulkLoadHFiles
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Row, SparkSession}
//
//
///**
// * @Author: deep as the sea
// * @Site: <a href="www.51doit.com">多易教育</a>
// * @QQ: 657270652
// * @Date: 2022/8/4
// * @Desc: 画像数据，用bulkload导入hbase ，测试
// *
// *
// *
// *
// *
// *        -- 测试用的hive中的某个画像标签表1
// *        hive> create table test_bulk_profile(
// *        guid int,
// *        age int,
// *        addr string,
// *        order_amt double
// *        )
// *        partitioned by (dt string)
// *        stored as orc
// *        tblproperties('orc.compress'='snappy')
// *        ;
// *
// *        insert into table test_bulk_profile partition(dt='2022-08-04')
// *        select  2,38,'shanghai',12600
// *        union all
// *        select  3,32,'beijing',14600
// *        union all
// *        select  4,31,'guangzhou',10600
// *
// *
// *
// *
// *        hive> create table test_bulk_profile2(
// *        guid     int,
// *        tag010   string,
// *        tag011   string,
// *        tag012   string,
// *        tag013   string,
// *        tag014   string,
// *        tag015   string,
// *        tag016   string
// *        )
// *        partitioned by (dt string)
// *        stored as orc
// *        tblproperties('orc.compress'='snappy')
// *        ;
// *
// *        insert into table test_bulk_profile2 partition(dt='2022-08-04')
// *        select  2,'a1','a2','a3','a4','a5','a6','a7'
// *        union all
// *        select  3,'b1','b2','b3','b4','b5','b6','b7'
// *        union all
// *        select  4,'c1','c2','c3','c4','c5','c6','c7'
// *        union all
// *        select  5,'d1','d2','d3','d4','d5','d6','d7'
// *
// *        -- 测试用的hbase中的用于存储画像标签的表（hbase中只需要一个表）
// *        hbase >   create 'doitedu_user_profile_tags', 'f'
// *
// * */
//
//object UserProfileBulkLoadTest {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("标签导入")
//      .master("local")
//      .enableHiveSupport()
//      .getOrCreate()
//
//
//    // 加载标签数据导入的配置参数
//    val hiveTableName = PropertiesHolder.getProperty("hive.table.name") // hive中的要导数据的表名
//    val fieldNames = PropertiesHolder.getProperty("hive.table.field.names").split(",") // hive中的表的各个要导入的标签字段名
//
//
//    // 先从数据源（hive）中读取到源数据
//    val tagDataframe = spark.read.table(hiveTableName)
//    /*
//        +----+---+---------+---------+----------+
//        |guid|age|addr     |order_amt|dt        |
//        +----+---+---------+---------+----------+
//        |2   |38 |shanghai |12600.0  |2022-08-04|
//        |3   |32 |beijing  |14600.0  |2022-08-04|
//        |4   |31 |guangzhou|10600.0  |2022-08-04|
//        +----+---+---------+---------+----------+
//     */
//
//    // 然后根据需要，对数据转化：   RDD[ (ImmutableBytesWriteble,  KeyValue ) ]
//    // 并且要根据   “行键,列族名,列名”  字典有序
//    val kvRdd: RDD[(ImmutableBytesWritable, KeyValue)] = {
//
//    // 根据配置，自适应各类hive表，进行转换
//    tagDataframe.rdd.flatMap(row => {
//      val guid = row.getAs[Int]("guid")
//      for (fieldName <- fieldNames) yield {
//        val fieldValue = row.getAs[String](fieldName)
//        (guid, "f", fieldName, fieldValue)
//      }
//    })
//      .sortBy(tp => (tp._1, tp._2, tp._3)) // 按照行键，列族名，列名排序
//      .map(tp => { // 映射成 hfile所要的kv元祖
//        (
//          new ImmutableBytesWritable(Bytes.toBytes(tp._1)), // 行键
//          new KeyValue(Bytes.toBytes(tp._1), Bytes.toBytes(tp._2), Bytes.toBytes(tp._3), Bytes.toBytes(tp._4)) // KeyValue
//        )
//      })
//    }
//
//
//    // 将转换好的rdd，调用api保存为HFILE文件即可
//    val conf = HBaseConfiguration.create()
//    conf.set("fs.defaultFS", "hdfs://doitedu:8020/")
//    conf.set("hbase.zookeeper.quorum", "doitedu:2181")
//
//    val job = Job.getInstance(conf)
//
//    val conn = ConnectionFactory.createConnection(conf)
//    val tableName = TableName.valueOf("doitedu_user_profile_tags")
//    val table = conn.getTable(tableName)
//    val locator = conn.getRegionLocator(tableName)
//
//    // 配置HfileOutputformat需要的信息
//    HFileOutputFormat2.configureIncrementalLoad(job, table, locator)
//
//    // 用hfileoutputformat来输出我们的rdd，成为hfile文件
//    kvRdd.saveAsNewAPIHadoopFile(
//      "hdfs://doitedu:8020/up_tags/2022-08-04/" + hiveTableName,
//      classOf[ImmutableBytesWritable],
//      classOf[KeyValue],
//      classOf[HFileOutputFormat2],
//      job.getConfiguration
//    )
//
//    spark.close()
//
//    // 用bulkload工具，将保存好的Hfile文件，导给hbase
//    val loader = BulkLoadHFiles.create(conf)
//    loader.bulkLoad(tableName, new Path("hdfs://doitedu:8020/up_tags/2022-08-04/" + hiveTableName))
//
//  }
//
//}
