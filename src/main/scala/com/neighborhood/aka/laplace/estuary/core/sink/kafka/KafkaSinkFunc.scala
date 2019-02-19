package com.neighborhood.aka.laplace.estuary.core.sink.kafka

import java.util.Properties

import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
  * Created by john_liu on 2019/2/19.
  *
  * @author neighborhood.aka.laplace
  */
abstract class KafkaSinkFunc[K, V](kafkaBean: KafkaBean[K, V]) extends SinkFunc {

  private lazy val kafkaProducer = buildKafkaProducer(kafkaBean)


  @volatile private var isRunning = false

  /**
    * 生命周期
    * 关闭
    */
  override def close: Unit = {
    Try(kafkaProducer.close())
    isRunning = false
  }

  /**
    * 生命周期
    * 开始
    */
  override def start: Unit = {
    kafkaProducer
    isRunning = true
  }

  /**
    * 检测，是否关闭
    *
    */
  override def isTerminated: Boolean = !isRunning


  /**
    * 获取topic
    * 当有自己特殊要求时，请override这个方法
    *
    * @param key 关键字
    * @return match topic
    */
  protected def getTopic(key: String = ""): String = kafkaBean.topic


  /**
    * 构建发送记录
    *
    * @param key
    * @param value
    * @return
    */
  protected def buildRecord(key: K, value: V): ProducerRecord[K, V] = {
    buildRecord(key, value, getTopic())
  }

  @inline
  protected def buildRecord(key: K, value: V, topic: String): ProducerRecord[K, V] = new ProducerRecord[K, V](topic, key, value)

  /**
    * 默认callback
    *
    * @param promise 使用promise 封装
    */
  private class defaultCallback(promise: Promise[RecordMetadata]) extends Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        promise.success(metadata)
      } else promise.failure(exception)
    }
  }

  /**
    * 发送数据
    *
    * @param key
    * @param value
    * @return
    */
  def send(key: K, value: V): Future[_] = {
    val promise = Promise[RecordMetadata]
    kafkaProducer.send(buildRecord(key, value), new defaultCallback(promise))
    promise.future
  }

  /**
    * 发送数据 自定义callback
    *
    * @param key
    * @param value
    * @param callback
    */
  def send(key: K, value: V, callback: Callback): Unit = {
    kafkaProducer.send(buildRecord(key, value), callback)
  }
}

object KafkaSinkFunc {

  //  val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(3))
  /**
    * @param kafkaBean
    * @return KafkaProducer
    *         根据kafkaBean的参数设置,初始化一个producer
    */
  def buildKafkaProducer[K, V](kafkaBean: KafkaBean[K, V]): KafkaProducer[K, V] = {
    val props = new Properties()
    props.putAll(KafkaBean.buildConfig(kafkaBean))
    new KafkaProducer[K, V](props)
  }

}