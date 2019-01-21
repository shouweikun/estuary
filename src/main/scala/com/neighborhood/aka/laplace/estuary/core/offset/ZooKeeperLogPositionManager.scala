package com.neighborhood.aka.laplace.estuary.core.offset

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.otter.canal.common.utils.JsonUtils
import com.alibaba.otter.canal.common.zookeeper.{ZkClientx, ZookeeperPathUtils}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.springframework.util.Assert

import scala.util.Try

/**
  * Created by john_liu on 2018/5/3.
  */
trait ZooKeeperLogPositionManager[T] extends LogPositionManager[T] {

  private var zkClientx: ZkClientx = null

  def start(): Try[Unit] = Try {

    Assert.notNull(zkClientx)
  }

  def stop(): Try[Unit] = Try {

  }

  def getLatestIndexBy(destination: String): T

  //todo
  def persistLogPosition(destination: String, logPosition: T): Unit = {

    val path = ZookeeperPathUtils.getParsePath(destination)
    val data = JsonUtils.marshalToByte(logPosition, SerializerFeature.BeanToArray)
    try
      zkClientx.writeData(path, data)
    catch {
      case e: ZkNoNodeException =>
        zkClientx.createPersistent(path, data, true)
    }
  }

  // ================== setter / getter =================
  def setZkClientx(zkClientx: ZkClientx): Unit = {
    this.zkClientx = zkClientx
  }

  def getZkClient = zkClientx

}
