package com.neighborhood.aka.laplace.estuary.core.util

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by john_liu on 2019/1/20.
  *
  * 简单的ringBuffer实现
  *
  * 并不线程安全
  * 并没有保证读写的原子性
  *
  * @tparam A 任意元素
  * @author neighborhood.aka.laplace
  *
  */
final class SimpleEstuaryRingBuffer[A: ClassTag](private val size: Int = 31)(implicit ev: TypeTag[A]) {
  private val length = size + 1
  private val buffer = new Array[A](length)
  @volatile private var head = 0; //读指针
  @volatile private var tail = 0; //写指针

  def isEmpty: Boolean = head == tail

  def isFull: Boolean = head == (tail + 1) % size

  def elemNum: Int = { //需要测试
    val diff = tail - head
    if (diff >= 0) diff
    else size + diff
  }

  def put(x: A): Boolean = if (isFull) false
  else {
    buffer(tail) = x
    tail = (tail + 1) % size
    true
  }


  def get: Option[A] = {
    val re = if (isEmpty) None else {
      val r = Option(buffer(head))
      buffer(head) = null.asInstanceOf[A]
      r
    }
    head = (head + 1) % size
    re
  }

  def foreach[B](f: A => B): Unit = while (!isEmpty) {
    get.map(f)
  }

  def clear = while (!isEmpty) {
    get
  }

  def peek: A = buffer(head)

}
