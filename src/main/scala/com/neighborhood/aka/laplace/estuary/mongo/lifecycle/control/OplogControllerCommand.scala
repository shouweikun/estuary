package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control

/**
  * Created by john_liu on 2019/3/6.
  */

sealed  trait  OplogControllerCommand
object OplogControllerCommand {

  case object OplogControllerRestart extends OplogControllerCommand

  case object OplogControllerStart extends OplogControllerCommand

  case object OplogControllerStopAndRestart extends OplogControllerCommand //针对 online时的人为重启

  case object OplogControllerCheckRunningInfo extends OplogControllerCommand

  case object  OplogControllerCollectChildInfo  extends OplogControllerCommand
}
