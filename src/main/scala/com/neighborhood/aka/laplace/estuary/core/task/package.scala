package com.neighborhood.aka.laplace.estuary.core

import akka.actor.Props

/**
  * Created by john_liu on 2018/5/9.
  */
package object task {

  sealed trait SyncTask {
    def props: Props

    def name: String

    def taskType: String = this.getClass.getName

    override def toString: String = {
      s"SyncTask,taskType:$taskType,prop:$props,name:$name"
    }
  }

  final case class Mysql2MysqlSyncTask(
                                        override val props: Props,
                                        override val name: String) extends SyncTask

  final case class Mongo2KafkaSyncTask(override val props: Props,
                                       override val name: String) extends SyncTask

}
