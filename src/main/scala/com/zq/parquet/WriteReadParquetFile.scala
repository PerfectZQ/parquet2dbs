package com.zq.parquet

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import parquet.column.ParquetProperties
import parquet.example.data.{Group, GroupFactory}
import parquet.example.data.simple.SimpleGroupFactory
import parquet.hadoop.{ParquetReader, ParquetWriter}
import parquet.hadoop.example.{GroupReadSupport, GroupWriteSupport}
import parquet.schema.{MessageType, MessageTypeParser}

object WriteReadParquetFile {

  def main(args: Array[String]): Unit = {

    val path = new Path("file:///Users/zhangqiang/IdeaProjects/init/data/people.parquet/part-00000-9ae4ea0b-e098-4b01-8bdf-55f9f54df45b-c000.snappy.parquet")

    // val path = new Path("file:///Users/zhangqiang/IdeaProjects/init/data/test.parquet")

    // writeParquetFile(path)

    readParquetFile(path)

  }

  def writeParquetFile(path: Path): Unit = {

    val schemaStr =
      s"""
         |message spark_schema {
         |  optional binary name (UTF8);
         |  optional int32 birthday (DATE);
         |  required int32 age;
         |  optional binary description (UTF8);
         |}
       """.stripMargin

    val nestSchemaStr =
      s"""
         |message avro_schema {
         |  required string a;
         |  required group nestArray1(LIST) {
         |    repeated group B {
         |      required string b;
         |      required group nestArray2(LIST) {
         |        repeated group C {
         |          required string c;
         |        }
         |      }
         |    }
         |  }
         |}
       """.stripMargin

    val schema: MessageType = MessageTypeParser.parseMessageType(schemaStr)

    val groupFactory: GroupFactory = new SimpleGroupFactory(schema)

    val sdf = new SimpleDateFormat("yyyy-MM-hh")

    val group: Group = groupFactory.newGroup()
      .append("name", "张强")
      .append("age", 23)
      .append("birthday", sdf.parse("1995-02-05").getTime.toInt)
      .append("description", "好人")
    // .append("decimal_data",)


    val parentPath = path.getParent.toUri.getPath

    val parquetFile = new File(s"$parentPath/${path.getName}")

    if (parquetFile.exists()) parquetFile.delete()

    val parquetCrcFile = new File(s"$parentPath/.${path.getName}.crc")

    if (parquetCrcFile.exists()) parquetCrcFile.delete()

    val configuration = new Configuration()

    GroupWriteSupport.setSchema(schema, configuration)

    val writeSupport: GroupWriteSupport = new GroupWriteSupport

    val parquetWriter: ParquetWriter[Group] = new ParquetWriter[Group](
      path,
      writeSupport,
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE, //dictionary page size
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetProperties.WriterVersion.PARQUET_2_0,
      configuration
    )

    parquetWriter.write(group)

    parquetWriter.close()

  }

  def readParquetFile(path: Path): Unit = {

    val groupReadSupport = new GroupReadSupport

    val readerBuilder: ParquetReader.Builder[Group] = ParquetReader.builder(groupReadSupport, path)

    val reader: ParquetReader[Group] = readerBuilder.build

    val result: Group = reader.read()

    println(
      s"""schema:
         |${result.getType.toString}""".stripMargin)

    //    println(
    //      s"""
    //         |name : ${result.getBinary("name", 0).toStringUsingUTF8}
    //         |age : ${result.getInteger("age", 0)}
    //         |birthday : ${new Date(result.getInteger("birthday", 0))}
    //       """.stripMargin)
  }
}
