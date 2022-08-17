//package cn.doitedu.profile
//
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
//import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, RegionLocator, Table}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
//import org.apache.hadoop.hbase.tool.BulkLoadHFiles
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.sql.{Row, SparkSession}
//
//object BulkloadDemo {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("")
//      .enableHiveSupport()
//      .master("local")
//      .config("spark.default.parallelism",1)
//      .getOrCreate()
//
//    // t.geohash  | t.province  | t.city  | t.region  |
//    val df = spark.read.table("dim.geohash_area").where("province is not null").distinct()
//    val rdd = df.rdd.map({
//      case Row(geohash: String, province: String, city: String, region: String)
//      => (geohash, "f", "q", province + "," + city + "," + region)
//    }).sortBy(tp=>(tp._1,tp._2,tp._3))
//      .map(tp=>{
//        (new ImmutableBytesWritable(tp._1.getBytes()),new KeyValue(tp._1.getBytes(),tp._2.getBytes(),tp._3.getBytes(),tp._4.getBytes()))
//      }).coalesce(1,false)
//
//
//    val conf = HBaseConfiguration.create()
//    conf.set("fs.defaultFS","hdfs://doitedu:8020/")
//    conf.set("hbase.zookeeper.quorum","doitedu:2181")
//    val job: Job = Job.getInstance(conf)
//
//    // 构造一个hbase的客户端
//    val conn: Connection = ConnectionFactory.createConnection(conf)
//    val tableName: TableName = TableName.valueOf("dim_geo_area")
//    val table: Table = conn.getTable(tableName)
//    val locator: RegionLocator = conn.getRegionLocator(tableName)
//
//    // HfileOutputFormat的参数配置
//    HFileOutputFormat2.configureIncrementalLoad(job,table,locator)
//
//
//    // 将rdd数据输出成Hfile文件
//    rdd.saveAsNewAPIHadoopFile("hdfs://doitedu:8020/tmp/geohash",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],job.getConfiguration)
//
//    // 将生成好的hfile导入到 hbase
//    val loader = BulkLoadHFiles.create(conf)
//    loader.bulkLoad(tableName,new Path("hdfs://doitedu:8020/tmp/geohash"))
//
//    // deprecated api
//    //val loader = new LoadIncrementalHFiles(conf)
//    //loader.doBulkLoad(new Path("hdfs://doitedu:8020/tmp/tags"), conn.getAdmin, table, conn.getRegionLocator(tableName))
//
//    table.close()
//    conn.close()
//    spark.close()
//
//
//  }
//
//}
