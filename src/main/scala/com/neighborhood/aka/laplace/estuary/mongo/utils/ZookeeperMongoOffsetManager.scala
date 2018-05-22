package com.neighborhood.aka.laplace.estuary.mongo.utils

import com.alibaba.otter.canal.common.utils.JsonUtils
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils
import com.alibaba.otter.canal.protocol.position.LogPosition
import com.neighborhood.aka.laplace.estuary.core.util.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.I0Itec.zkclient.ZkClient

/**
  * Created by john_liu on 2018/5/6.
  */
class ZookeeperMongoOffsetManager extends ZooKeeperLogPositionManager[MongoOffset] {
  override def getLatestIndexBy(destination: String): MongoOffset = {
    val path = ZookeeperPathUtils.getParsePath(destination)
    val data: Array[Byte] = this.zkClientx.readData(path, true)

    if (data == null || data.length == 0) return null

    return JsonUtils.unmarshalFromByte(data, classOf[MongoOffset])
  }
}
