package com.neighborhood.aka.laplace.estuary.core

import akka.actor.Props

/**
  * Created by john_liu on 2018/5/9.
  */
package object task {

  trait SyncTask {
    val prop: Props
    val name: Option[String]

    def taskType: String = this.getClass.getName

    override def toString: String = {
      s"SyncTask,taskType:$taskType,prop:$prop,name:$name"
    }
  }

  case class Mysql2KafkaInOrderTask(override val prop: Props, override val name: Option[String]) extends SyncTask

  case class Mysql2KafkaTask(override val prop: Props, override val name: Option[String]) extends SyncTask

  case class Mongo2KafkaTask(override val prop: Props, override val name: Option[String]) extends SyncTask


}
