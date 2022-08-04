package cn.doitedu.profile

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

object BulkloadDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.makeRDD(Seq((1, "zs"), (2, "ls")))
    val tagsRdd = rdd
      .sortBy(tp=>tp._1)
      .map(tp=>{
      (new ImmutableBytesWritable(Bytes.toBytes(tp._1)),new KeyValue(Bytes.toBytes(tp._1),"f".getBytes(),"n".getBytes(),tp._2.getBytes()))
    })


    val conf = HBaseConfiguration.create()
    conf.set("fs.defaultFS","hdfs://doitedu:8020/")
    conf.set("hbase.zookeeper.quorum","doitedu:2181")
    val job: Job = Job.getInstance(conf)

    // 构造一个hbase的客户端
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val tableName: TableName = TableName.valueOf("tags")
    val table: Table = conn.getTable(tableName)
    val locator: RegionLocator = conn.getRegionLocator(tableName)

    // HfileOutputFormat的参数配置
    HFileOutputFormat2.configureIncrementalLoad(job,table,locator)


    // 将rdd数据输出成Hfile文件
    tagsRdd.saveAsNewAPIHadoopFile("hdfs://doitedu:8020/tmp/tags2",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],job.getConfiguration)

    // 将生成好的hfile导入到 hbase
    val loader = BulkLoadHFiles.create(conf)
    loader.bulkLoad(tableName,new Path("hdfs://doitedu:8020/tmp/tags2"))

    // deprecated api
    //val loader = new LoadIncrementalHFiles(conf)
    //loader.doBulkLoad(new Path("hdfs://doitedu:8020/tmp/tags"), conn.getAdmin, table, conn.getRegionLocator(tableName))

    table.close()
    conn.close()
    spark.close()


  }

}
