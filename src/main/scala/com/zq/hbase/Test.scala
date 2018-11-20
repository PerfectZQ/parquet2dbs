package com.zq.hbase

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession


object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Spark HBase Write Demo").getOrCreate()
    val sc = spark.sparkContext

    val tableName = "test_table"

    // JobConf 是 write out
    val jobConf = new JobConf(this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // Note: TableOutputFormat 是 mapred 包下的
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
    jobConf.set("zookeeper.znode.parent", "/hbase")

    // (rowkey, value)
    val pairs = sc.parallelize(Seq("2018/4/27-rowkey" -> "value"))

    def convert(pair: (String, String)) = {
      val (rowkey, value) = pair
      val p = new Put(Bytes.toBytes(rowkey))
      // column family, column qualifier, value
      p.addColumn(Bytes.toBytes("column_family"), Bytes.toBytes("column_qualifier"), Bytes.toBytes(value))
      (new ImmutableBytesWritable, p)
    }

    val connection = HBase.getConnection("47.94.4.250,39.107.122.200,39.107.110.81", isCDH = true)
    HBase.createTableIfNotExists(tableName, "column_family", connection)

    // 保存数据到 HBase
    new PairRDDFunctions(pairs.map(convert)).saveAsHadoopDataset(jobConf)
  }
}
