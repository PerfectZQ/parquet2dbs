package com.zq.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}

object HBase {
  /**
    *
    * @param tableName
    * @return
    */
  def tableExists(tableName: String,
                  zookeeperQuorum: String,
                  zookeeperPort: String = "2181",
                  isCDH: Boolean = false): Boolean = {
    val connection: Connection = getConnection(zookeeperQuorum, zookeeperPort, isCDH)
    /**
      * 通过连接获取管理对象
      * Note:
      * 1、这是一个轻量级的操作，不会保证返回的Admin对象是线程安全的
      * 在多线程的情况下，应该为每个线程创建一个新实例。
      * 2、不建议把返回的 Admin 对象池化或缓存
      * 3、记得调用 admin.close()
      */
    val admin: Admin = connection.getAdmin
    val isExists = admin.tableExists(TableName.valueOf(tableName))
    admin.close()
    connection.close()
    isExists
  }

  /**
    * 获取 HBase connection 连接
    *
    * @param zookeeperQuorum zookeeper 仲裁节点
    * @param zookeeperPort   zookeeper 端口
    * @param isCDH           是否是 CDH 集群
    * @return
    */
  def getConnection(zookeeperQuorum: String,
                    zookeeperPort: String = "2181",
                    isCDH: Boolean = false): Connection = {
    // 创建 HBase 配置文件对象
    val conf = HBaseConfiguration.create()
    // 指定 zookeeper 节点地址，在 hbase-site.xml 有配置
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    // 指定 zookeeper 端口号
    conf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    // 如果是 hdp 需要修改一下 zookeeper.znode.parent
    if (!isCDH) conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    else conf.set("zookeeper.znode.parent", "/hbase")
    // 根据配置文件创建连接
    ConnectionFactory.createConnection(conf)
  }

  /**
    * 如果表不存在，则创建表
    *
    * @param tableName
    * @param columnFamilies 列簇 e.g.: family1,family2
    * @param connection
    */
  def createTableIfNotExists(tableName: String,
                             columnFamilies: String,
                             connection: Connection): Unit = {
    val admin = connection.getAdmin
    try {
      val table = TableName.valueOf(tableName)
      if (!admin.tableExists(table)) {
        val tableDescriptor: HTableDescriptor = new HTableDescriptor(table)
        if (columnFamilies != null && columnFamilies.trim != "") {
          val families = columnFamilies.split(",")
          families.foreach { family =>
            val columnDescriptor: HColumnDescriptor = new HColumnDescriptor(family.getBytes())
            tableDescriptor.addFamily(columnDescriptor)
          }
        }
        admin.createTable(tableDescriptor)
      }
    } finally {
      admin.close()
    }
  }
}
