package com.neighborhood.aka.laplace.estuary.core.sink.hdfs

import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/4/4.
  */
trait HdfsSinkFunc[A] extends SinkFunc {

  protected lazy val logger = LoggerFactory.getLogger(classOf[HdfsSinkFunc[A]])
  protected lazy val fs: FileSystem = initFileSystem

  protected def basePath: String = ""

  protected lazy val connectStatus = new AtomicBoolean(false)

  /**
    * 初始化HDFS系统
    *
    * @return FileSystem
    */
  def initFileSystem: FileSystem = {
    val hadoopConf: org.apache.hadoop.conf.Configuration = ???
    FileSystem.get(hadoopConf)
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
  override def isTerminated: Boolean = connectStatus.get()
}
