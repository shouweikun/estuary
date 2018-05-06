package com.neighborhood.aka.laplace.estuary.core.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.otter.canal.common.utils.JsonUtils
import com.alibaba.otter.canal.common.zookeeper.{ZkClientx, ZookeeperPathUtils}
import com.alibaba.otter.canal.protocol.position.LogPosition
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.springframework.util.Assert

import scala.reflect.ClassTag

/**
  * Created by john_liu on 2018/5/3.
  */
trait ZooKeeperLogPositionManager[T] {

    var zkClientx: ZkClientx = null

  def start(): Unit = {

    Assert.notNull(zkClientx)
  }

  def stop(): Unit = {

  }

  def getLatestIndexBy(destination: String): T

  def persistLogPosition(destination: String, logPosition: T): Unit = {
    val path = ZookeeperPathUtils.getParsePath(destination)
    //todo
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
