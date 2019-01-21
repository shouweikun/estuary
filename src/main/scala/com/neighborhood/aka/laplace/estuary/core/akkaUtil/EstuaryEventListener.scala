package com.neighborhood.aka.laplace.estuary.core.akkaUtil


import akka.actor.{Actor, ActorLogging, Props}
import akka.event.Logging._
import com.neighborhood.aka.laplace.estuary.core.util.message.MessageSender
import com.typesafe.config.ConfigFactory

class EstuaryEventListener extends Actor with ActorLogging {

  val TIME_INTERVAL = 5 * 60 * 1000 //5min 以ms为单位
  val config = ConfigFactory.load()
  val url = config.getString("error.monitor.url")
  val mobilelist = List(config.getString("error.monitor.mobiles"))
  val ip = if (config.hasPath("app.server.ip")) config.getString("app.server.ip") else "unknown"
  val port = if (config.hasPath("app.server.port")) config.getInt("app.server.port") else -1
  var lastSendTime = 0l

  override def receive = {
    case InitializeLogger(_) => sender() ! LoggerInitialized

    case Error(cause, logSource, logClass, message) => {
      def buildAndSendErrorMessage = {
        //      信息内容
        lazy val contents = List(s"exception:${cause},cause:${cause.getCause},logSource:$logSource,logClass:$logClass,message $message,host:$ip:$port")
        MessageSender.sendMessageReturnWithinString(contents, mobilelist)(url).toOption
          .fold(log.error(s"message send failure"))(_ => log.info("message send success"))
      }

      lazy val now = System.currentTimeMillis()
      if (now - lastSendTime > TIME_INTERVAL) {
        buildAndSendErrorMessage
        lastSendTime = now
      }

    }
  }
}

object EstuaryEventListener {
  def props: Props = Props(new EstuaryEventListener)
}