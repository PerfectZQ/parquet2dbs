package com.zq.elasticsearch

import java.io.File
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

object FPSParquet2ES {

  private lazy val logger = Logger.getLogger(this.getClass)

  private val dateTypes = Set("date", "timestamp")

  case class People(name: String,
                    birthday: Timestamp,
                    age: Int,
                    description: String,
                    decimal: BigDecimal)

  def main(args: Array[String]): Unit = {

    val esProps = new Properties()
    esProps.load(this.getClass.getClassLoader.getResourceAsStream("elasticsearch.properties"))

    val conf = new SparkConf()
    conf.set("es.index.auto.create", esProps.getProperty("es.index.auto.create"))
    conf.set("es.nodes", esProps.getProperty("es.nodes"))
    conf.set("es.port", esProps.getProperty("es.port"))

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Parquet to elasticsearch")
      .enableHiveSupport()
      .config(conf)
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
      People("李墨竹", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1),
      People("李墨竹1", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1),
      People("李墨竹2", new Timestamp(birthday2), 24, "那我可真个是王八蛋啊！", 1)
    )

    FileUtils.deleteDirectory(new File("data/people.parquet"))


    sparkContext
      .makeRDD(test)
      .toDF
      .write
      .parquet("data/people.parquet")

    // nest parquet 文件
    sparkSession.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS orc_qiche (
         |    query string,
         |    uid string,
         |    country string,
         |    province string,
         |    city string,
         |    sttime string,
         |    properties map<string, array<string>>,
         |    intension array<string>
         |    )
       """.stripMargin)


    parquetToElasticIndex(
      "data/people.parquet",
      "Test",
      "db-es1-node1-pub:9202",
      "name,birthday",
      null, //"name:ngram,description:ik_max_word,description:ik_smart",
      "skip",
      deleteIndexOnExist = true)

  }

  /**
    *
    * @param parquetFile
    * @param indexName
    * @param elasticsearchAddress
    * @param analyserConf e.g.: column1:analyser1,column2:analyser2
    */
  def parquetToElasticIndex(parquetFile: String,
                            indexName: String,
                            elasticsearchAddress: String,
                            primaryId: String,
                            analyserConf: String,
                            strategy: String,
                            deleteIndexOnExist: Boolean = false): Unit = {

    // Elasticsearch index must be lowercase
    val realIndexName = indexName.toLowerCase()

    // must not start with `_`, `-`, or `+`
    if (("^[a-z0-9.][a-z0-9._-]*$".r findFirstIn realIndexName).isEmpty) {
      throw new IllegalArgumentException(s"======= Invalid index name: $realIndexName")
    }

    val esAddressInfo = elasticsearchAddress.split(":")
    val url = esAddressInfo(0)
    var port = 9200

    if (esAddressInfo.size > 1) port = esAddressInfo(1).toInt

    val conf = new SparkConf()
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", url)
    conf.set("es.port", port.toString)

    val sparkSession = SparkSession.builder()
      // .master("local[*]")
      .appName("Parquet to elasticsearch")
      .config(conf)
      .getOrCreate

    var parquetFileDF = sparkSession.read.parquet(parquetFile)

    parquetFileDF.printSchema()

    import org.elasticsearch.spark.sql._

    val es = new ElasticSearch(url, port)

    if (!es.indexExists(realIndexName)) {
      logger.info(s"======= 创建索引 $realIndexName ...")
      initIndex(realIndexName, es)
    } else {
      logger.info(s"======= 索引 $realIndexName 已经存在！")
      if (deleteIndexOnExist) {
        logger.info(s"======= deleteIndexOnExist = $deleteIndexOnExist, 删除旧索引 $realIndexName")
        es.deleteIndex(realIndexName)
        initIndex(realIndexName, es)
      }
    }


    if (analyserConf != null && analyserConf.trim != "") {
      // es field must not start with `.`
      if (("""^[^.:,][^:,]*:\w+([,][^.:,][^:,]*:\w+)*$""".r findFirstIn analyserConf.trim).nonEmpty) {
        val analysers = analyserConf.split(",")
        analysers.foreach { str =>
          val Array(field, analyser) = str.split(":")
          requireFieldsExists(parquetFileDF.schema, field)
          if (!isStringType(parseFieldType(parquetFileDF.schema, field))) {
            throw new IllegalArgumentException(s"======= 检测到非 string 类型的字段：$field，只有 string 类型字段才能被分词！ ")
          } else {
            es.configureAnalyser(realIndexName, field, analyser)
          }
        }
      } else {
        throw new RuntimeException("======= AnalyserConf 格式有误！")
      }
    }

    val config = new mutable.HashMap[String, String]()

    if (primaryId != null && primaryId.trim != "") {
      val primaryFields = primaryId.split(",")
      requireFieldsExists(parquetFileDF.schema, primaryFields: _*)
      // id 列名称
      var primaryCol = ""
      if (primaryFields.size == 1) {
        primaryCol = primaryFields.head
        config.put("es.mapping.id", primaryCol)
      }
      else {
        primaryCol = "uniqueId"
        parquetFileDF = parquetFileDF.selectExpr(s"md5(concat(${primaryFields.mkString(",")})) as uniqueId", "*")
        config.put("es.mapping.id", primaryCol)
      }

      /**
        * default: replace the new record when primary id is duplicated
        * skip: ignore the new record when primary id is duplicated
        * upsert: merge and update the new record when primary id is duplicated
        */
      strategy match {
        case "skip" =>
          logger.info("======= strategy = skip ")
          val schema = parquetFileDF.schema
          val processRDD = parquetFileDF.rdd.mapPartitions { partition =>
            val restClient = es.getRestClient()
            var result = Seq[Row]()
            try {
              // Iterator 的算子都是懒加载的，不转成 Seq 会提前关闭 restClient
              result = partition.toSeq.filterNot { row => es.documentExists(restClient, realIndexName, row.getAs[String](primaryCol)) }
            } finally {
              restClient.close()
            }
            result.toIterator
          }
          parquetFileDF = sparkSession.createDataFrame(processRDD, schema)
          parquetFileDF = parquetFileDF.distinct()
        case "upsert" =>
          logger.info("======= strategy = upsert ")
          config.put("es.write.operation", "upsert")
        case _ =>
      }
    }

    // spark udf
    val formatTimestamp = (timestamp: Timestamp) => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      if (timestamp != null) sdf.format(timestamp)
      else sdf.format(new Timestamp(0L))
    }

    sparkSession.udf.register("formatTimestamp", formatTimestamp)

    val fieldExprs: Seq[String] = parquetFileDF.schema.map {
      // suffix `__date` indicates the field will be stored as date type in ElasticSearch
      case structField if isDateType(structField.dataType.typeName) =>
        //s"${structField.name} as ${structField.name}__date"
        es.configureDateTypeMapping(realIndexName, structField.name)
        s"formatTimestamp(${structField.name}) as ${structField.name}"
      // ElasticSearch doesn't support decimal, cast to double
      case structField if structField.dataType.typeName.contains("decimal") =>
        es.configureDoubleTypeMapping(realIndexName, structField.name)
        s"cast(${structField.name} as double)"
      case structField => structField.name
    }

    parquetFileDF = parquetFileDF.selectExpr(fieldExprs: _*)
    parquetFileDF.saveToEs(s"$realIndexName/$realIndexName", config)
  }

  /**
    * 初始化索引
    *
    * @param indexName
    * @param es
    */
  def initIndex(indexName: String, es: ElasticSearch): Unit = {
    if (es.createIndex(indexName)) {
      // es.configureIndexWithDateMatch(indexName)
      logger.info(s"======= 索引 $indexName 创建成功！")
    } else {
      logger.error(s"======= 索引 $indexName 创建失败！")
      throw new RuntimeException(s"索引 $indexName 创建失败！")
    }
  }

  /**
    * 保证更改的字段在 schema 中存在，否则抛出异常
    *
    * @param schema 表结构
    * @param fields 字段名
    */
  def requireFieldsExists(schema: StructType, fields: String*): Unit = {
    val diffFields = fields.diff(schema.fieldNames)
    if (diffFields.nonEmpty)
      throw new IllegalArgumentException(s"======= 引用了不存在的列：${diffFields.mkString(",")}")
  }

  /**
    * 根据 schema 解析字段类型
    *
    * @param schema 表结构
    * @param field  字段名
    * @return
    */
  def parseFieldType(schema: StructType, field: String): String = {
    val result = schema.filter(_.name == field)
    if (result.nonEmpty) result.head.dataType.typeName
    else null
  }

  /**
    * 判断字段是否是 Date 类型
    *
    * @param typeName
    * @return
    */
  def isDateType(typeName: String): Boolean = {
    dateTypes.contains(typeName)
  }

  /**
    * 判断字段是否是 String 类型
    *
    * @param typeName
    * @return
    */
  def isStringType(typeName: String): Boolean = typeName == "string"
}
