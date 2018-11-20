package com.zq.parquet

import org.apache.spark.sql.SparkSession

object HiveParquet {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("hive parquet test")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    sparkSession.sql(
      s"""
         |CREATE TABLE parquet_test (
         |   id int,
         |   str string,
         |   mp MAP<STRING,STRING>,
         |   lst ARRAY<STRING>,
         |   strct STRUCT<A:STRING,B:STRING>)
         |PARTITIONED BY (part string)
         |STORED AS PARQUET;
       """.stripMargin)
  }
}
