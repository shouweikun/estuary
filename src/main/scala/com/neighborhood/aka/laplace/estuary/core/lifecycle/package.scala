package com.neighborhood.aka.laplace.estuary.core

/**
  * Created by john_liu on 2018/2/9.
  */
package object lifecycle {

  sealed trait WorkerMessage{def msg: Any}

  final case class SyncControllerMessage(override val msg: Any) extends WorkerMessage

  final case class ListenerMessage(override val msg: Any) extends WorkerMessage

  final case class SinkerMessage(override val msg: Any) extends WorkerMessage

  final case class FetcherMessage(override val msg: Any) extends WorkerMessage

  final case class BatcherMessage(override val msg: Any) extends WorkerMessage

  final case class RecorderMessage(override val msg: Any) extends WorkerMessage

  final case class SnapshotJudgerMessage(override val msg: Any) extends WorkerMessage

  final case class PowerAdapterMessage(override val msg: Any) extends WorkerMessage

}
