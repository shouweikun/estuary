package com.neighborhood.aka.laplace.estuary.core.schema

import com.neighborhood.aka.laplace.estuary.bean.datasink.HBaseBean
import com.neighborhood.aka.laplace.estuary.core.schema.HBaseEventualSinkSchemaHandler.HBaseTableInfo
import org.apache.hadoop.hbase.{HBaseConfiguration, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder}

/**
  * Created by john_liu on 2018/6/1.
  */
class HBaseEventualSinkSchemaHandler(
                                      hBaseBean: HBaseBean
                                    ) extends EventualSinkSchemaHandler[HBaseTableInfo] {


  val conf = HBaseConfiguration.create()
  //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  conf.set("hbase.zookeeper.quorum", s"${hBaseBean.HbaseZookeeperQuorum}")
  //    //设置zookeeper连接端口，默认2181
  conf.set("hbase.zookeeper.property.clientPort", s"${hBaseBean.HabseZookeeperPropertyClientPort}")
  lazy val conn = ConnectionFactory.createConnection(conf)
  lazy val admin = conn.getAdmin
  lazy val originalColumnFamily: Array[Byte] = "original".getBytes
  lazy val cifColumnFamily: Array[Byte] = "cif".getBytes

  /**
    * 创建db
    */
  override def createDb(info: HBaseTableInfo): Unit = {
    admin.createNamespace(NamespaceDescriptor.create(s"${info.nameSpaceName}").build())

  }

  /**
    * 删除db
    */
  override def dropDb(info: HBaseTableInfo): Unit = {
    admin.deleteNamespace(s"${info.nameSpaceName}")
  }

  /**
    * 创建表
    * HBase表分成两个列族：
    * columnFamily1 是原来数据的信息
    * columnFamily2 程序附加的信息 例如：时间戳，版本等
    */
  override def createTable(info: HBaseTableInfo): Unit = {

    lazy val tableName = TableName.valueOf(info.nameSpaceName.getBytes, info.tableName.getBytes)
    lazy val desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(originalColumnFamily))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cifColumnFamily))
      .build()
    admin.createTable(desc);
  }

  /**
    * 删除表
    */
  override def dropTable(info: HBaseTableInfo): Unit = {
    lazy val tableName = TableName.valueOf(info.nameSpaceName.getBytes, info.tableName.getBytes)
    admin.
  }

  /**
    * 查看库表是否存在
    */
  override def isExists(info: HBaseTableInfo): Boolean = ???
}

object HBaseEventualSinkSchemaHandler {

  case class HBaseTableInfo(nameSpaceName: String, tableName: String)

}