package com.zq.log

import java.io.File
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WriteLog {

  private lazy val logger = Logger.getLogger(this.getClass)

  private val DATE_TYPES_NAMES = Set("date", "timestamp")

  case class Person(name: String,
                    birthday: Date,
                    ip: String)

  case class Cat(kind: String,
                 master: String,
                 age: Int)

  def main(args: Array[String]): Unit = {

    val birthday = new SimpleDateFormat("yyyy-MM-dd")
      .parse("1994-03-10").getTime

    val person = Person("李墨竹", new Date(birthday), "192.168.10.1")
    val cat = Cat("蓝猫", "李墨竹", 1)
    while (true) {

    }
    Range(0, 250).foreach(x => logger.info(s"${person.name} ${person.birthday} ${person.ip}"))
    Range(0, 250).foreach(x => logger.info(s"${cat.kind} ${cat.master} ${cat.age}"))

  }

}
