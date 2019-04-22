package com.neighborhood.aka.laplace.estuary.core.sink.hdfs

import java.io.BufferedInputStream
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.bean.support.HdfsMessage
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/4/4.
  */
trait HdfsSinkFunc extends SinkFunc {

  protected lazy val logger = LoggerFactory.getLogger(classOf[HdfsSinkFunc])
  protected lazy val fs: FileSystem = initFileSystem

  protected def basePath: String = "/user/mongo_sync/mongo_backups"

  protected lazy val connectStatus = new AtomicBoolean(false)

  private lazy val outputHolder = new ConcurrentHashMap[String, (String, FSDataOutputStream)]()

  /**
    * 初始化HDFS系统
    *
    * @return FileSystem
    */
  def initFileSystem: FileSystem = {
    val hadoopConf: org.apache.hadoop.conf.Configuration = new Configuration()
    hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    val HDFS_PATH = "hdfs://p2.hadoop.dc.finupgroup.com:8020";
    FileSystem.get(new URI(HDFS_PATH), hadoopConf, "mongo_sync");
  }

  override def close: Unit = this.synchronized {
    if (connectStatus.compareAndSet(false, true)) {
      logger.info("close hdfs sink ")
      fs.close()
    } else {
      logger.warn("no need to close HdfsSinkFunc,cause it has been closed")
    }
  }

  /**
    * 生命周期
    * 开始
    */
  override def start: Unit = this.synchronized {
    if (connectStatus.compareAndSet(false, true)) {
      logger.info("start hdfs sink ")
      fs
    } else {
      logger.warn("no need to start HdfsSinkFunc,cause it has been started")
    }
  }

  /**
    * 检测，是否关闭
    *
    */
  override def
  isTerminated: Boolean = connectStatus.get()

  def send(hdfsMessage: HdfsMessage[MongoOffset], fsDataOutputStream: FSDataOutputStream) = {
    fsDataOutputStream.write(hdfsMessage.toString.getBytes);
    fsDataOutputStream.write("\n".getBytes());
  }

  /**
    * 获取插入hdfs的stream
    *
    * @param dbName
    * @param tableName
    * @param ts
    * @return
    */
  def getOutputStream(dbName: String, tableName: String, ts: Int, oldOutputStream: Option[FSDataOutputStream] = None): FSDataOutputStream = {
    val key = s"$dbName.$tableName"
    val format = new SimpleDateFormat("yyyyMMdd")
    val nowdate = format.format(new Date(ts * 1000l))
    //    logger.info(s"*********key:$key,nowdate:$ts,outputHolder:${outputHolder.toString}")
    if (oldOutputStream.isEmpty) {
      val outputStream = outputHolder.get(key)
      if (outputStream._1 != nowdate) {
        outputStream._2.flush()
        outputStream._2.close()
        val path = new Path(s"$basePath/$dbName/$tableName/$nowdate/${System.currentTimeMillis()}")
        val newOutput = fs.create(path);
        outputHolder.put(key, (nowdate, newOutput))
        logger.info(s"create newOutput-day key:$key,,nowdate:$nowdate,ts:$ts")
        newOutput
      } else {
        outputStream._2
      }
    } else {
      val path = new Path(s"$basePath/$dbName/$tableName/$nowdate/${System.currentTimeMillis()}")
      val newOutput = fs.create(path);
      outputHolder.put(key, (nowdate, newOutput))
      logger.info(s"create newOutput key:$key,,nowdate:$nowdate,ts:$ts")
      newOutput
    }
  }

}
