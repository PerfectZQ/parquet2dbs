package com.zq.hbase

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.zq.elasticsearch.FPSParquet2ES.People
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.{Row, SparkSession}

object Parquet2HBase {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate

    val sparkContext = sparkSession.sparkContext

    val birthday1 = new SimpleDateFormat("yyyy-MM-dd")
      .parse("1995-02-05").getTime
    val birthday2 = new SimpleDateFormat("yyyy-MM-dd")
      .parse("1994-03-10").getTime

    import sparkSession.implicits._

    // 普通 parquet 文件
    val test = Array(
      People("张强", new Timestamp(birthday1), 1223, "我是好人！", 1),
      People("李墨竹", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1),
      People("李墨竹3", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1),
      People("李墨竹1", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1),
      People("李墨竹2", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1)
    )

    FileUtils.deleteDirectory(new File("data/people.parquet"))


    sparkContext
      .makeRDD(test)
      .toDF
      .write
      .parquet("data/people.parquet")

    parquet2HBase(
      "data/people.parquet",
      "test",
      null,
      "name",
      "ambari3,ambari1,ambari2"
    )
  }

  /**
    * 将 parquet 文件写入指定 hbase 表中
    *
    * @param parquetFile     parquet 文件路径
    * @param tableName       要写入的 hbase 表名
    * @param columnFamily    指定列簇名，如果为空则为`uc`
    * @param rowKeyColumn    指定 rowkey 列
    * @param zookeeperQuorum zookeeper 仲裁者
    */
  def parquet2HBase(parquetFile: String,
                    tableName: String,
                    columnFamily: String,
                    rowKeyColumn: String,
                    zookeeperQuorum: String): Unit = {

    // 如果为true，zookeeper.znode.parent=/hbase-unsecure，否则/hbase
    val isAmbari: Boolean = false

    val sparkSession = SparkSession
      .builder()
      .appName("Parquet to HBase")
      .getOrCreate()

    val parquetFileDF = sparkSession.read.parquet(parquetFile)

    val connection = HBase.getConnection(zookeeperQuorum, isCDH = !isAmbari)
    if (columnFamily != null && columnFamily.trim != "") {
      HBase.createTableIfNotExists(tableName, columnFamily, connection)
    } else {
      HBase.createTableIfNotExists(tableName, "uc", connection)
    }

    val rdd = parquetFileDF.rdd
      .map { row =>
        row.schema.fieldNames.foreach(println)
        if (columnFamily != null && columnFamily.trim != "")
          convert(rowKeyColumn, row, columnFamily)
        else
          convert(rowKeyColumn, row)
      }

    val jobConf = new JobConf(this.getClass)

    /**
      * hdp hbase 需要指定 znode.parent 为 /hbase-unsecure，而不是
      * 默认的 /hbase ，否则 ZooKeeperWatcher.getMetaReplicaNodes
      * 会报空指针异常
      */
    if (isAmbari) jobConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    else jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    new PairRDDFunctions(rdd).saveAsHadoopDataset(jobConf)
  }

  /**
    * 将 Row 转换成可写入 hbase 的二元组
    *
    * @param rowkey
    * @param row
    * @param columnFamily
    * @return
    */
  def convert(rowkey: String, row: Row, columnFamily: String = "uc"): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(row.getAs[String](rowkey)))
    row.schema.toIterator
      .filterNot(_.name == rowkey)
      .foreach { structField =>
        val fieldName = structField.name
        val fieldType = structField.dataType.typeName
        val index = row.fieldIndex(fieldName)
        var value: String = null
        if (row.isNullAt(index)) value = ""
        else
          value = fieldType match {
            case "timestamp" => row.getAs[Timestamp](fieldName).getTime.toString
            case _ => row.get(index).toString
          }
        p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(fieldName), Bytes.toBytes(value))
      }
    (new ImmutableBytesWritable, p)
  }

}
