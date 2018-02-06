package com.neighborhood.aka.laplce.estuary.mysql

import java.util.concurrent.atomic.AtomicLong
import javax.swing.event.DocumentEvent.EventType

import akka.actor.{Actor, ActorLogging}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplce.estuary.mysql.actors.BatcherMessage

import scala.annotation.tailrec

/**
  * Created by john_liu on 2018/2/6.
  */
class BinlogTransactionBufferSinker extends Actor with ActorLogging{
  private val INIT_SQEUENCE = -1
  private var bufferSize = 1024
  private var indexMask = 0
  private var entries: Array[CanalEntry.Entry] = Array.empty
  private val putSequence = new AtomicLong(INIT_SQEUENCE) // 代表当前put操作最后一次写操作发生的位置
  private val flushSequence = new AtomicLong(INIT_SQEUENCE) // 代表满足flush条件后最后一次数据flush的时间

  //offline
  override def receive: Receive = {
    case BatcherMessage(msg) => {
      msg match {
        case "start" =>{
          preBuffer
          context.become(online)

        }
      }
    }
  }
  def online:Receive = {

  }
  def preBuffer = {
    if (Integer.bitCount(bufferSize) != 1) throw new IllegalArgumentException("bufferSize must be a power of 2")
    indexMask = bufferSize - 1

  }

  def afterBuffer = {
    putSequence.set(INIT_SQEUENCE)
    flushSequence.set(INIT_SQEUENCE)
    //可能会有问题
    entries = new Array(bufferSize)


  }

  def add(entry: CanalEntry.Entry): Unit = {
    entry.getEntryType match {
      case EntryType.TRANSACTIONBEGIN => {
        flush() // 刷新上一次的数据
        put(entry)
      }

      case EntryType.TRANSACTIONEND => {
        put(entry)
        flush()
      }
      case EntryType.ROWDATA => {
        put(entry)
      }
    }
  }

  def add(list: List[CanalEntry.Entry]): Unit = {
    list.foreach(add(_))

  }

  def reset(): Unit = {
    putSequence.set(INIT_SQEUENCE)
    flushSequence.set(INIT_SQEUENCE)
  }

  @tailrec
  final def put(data: CanalEntry.Entry): Unit = {
    //首先检查是否有空位
    if (checkFreeSlotAt(putSequence.get() + 1)) {
      val current = putSequence.get()
      val next = current + 1
      //先写数据，在更新对应的cursor，并发度高的情况，putSequence会被get请求课件，拿出了ringbuffer中的老的Entry值
      entries(getIndex(next)) = data
      putSequence.set(next)
    } else {
      flush()
      put(data)
    }
  }

  private def flush(): Unit = {
    val start = flushSequence.get() + 1
    val end = putSequence.get()

    if(start <= end) {
      (start to end)
        .map(x=> entries(getIndex(x)))
      .toList
    }
  }

  private def checkFreeSlotAt(sequence: Long): Boolean = {
    val wrapPoint = sequence - bufferSize
    if (wrapPoint > flushSequence.get()) false else true
  }

  private def getIndex(sequence: Long): Int = {
    (sequence & indexMask).asInstanceOf[Int]

  }

}
