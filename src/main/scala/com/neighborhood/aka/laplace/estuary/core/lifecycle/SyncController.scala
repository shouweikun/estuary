package com.neighborhood.aka.laplace.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/2/6.
  */
trait SyncController extends worker{
  implicit val workerType = WorkerType.SyncController
}
