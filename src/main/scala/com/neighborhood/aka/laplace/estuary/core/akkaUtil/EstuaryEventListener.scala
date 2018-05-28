package com.neighborhood.aka.laplace.estuary.core.akkaUtil


import akka.actor.{Actor, ActorLogging, Props}
import akka.event.Logging._
import com.typesafe.config.ConfigFactory
import org.springframework.http.{HttpHeaders, MediaType}
import org.springframework.web.client.RestTemplate

import scala.collection.mutable

class EstuaryEventListener extends Actor with ActorLogging {

  val TIME_INTERVAL = 5 * 60 * 1000 //5min 以ms为单位
  val config = ConfigFactory.load()
  val url = config.getString("error.monitor.url")
  val mobilelist = List(config.getString("error.monitor.mobiles"))
  val restTemplate = new RestTemplate
  val headers = new HttpHeaders
  var lastSendTime = 0l

  override def receive = {
    case InitializeLogger(_) => sender() ! LoggerInitialized

    case Error(cause, logSource, logClass, message) => {
      def buildandSendErrorMessage = {
        //    消息体
        lazy val messageBody = new MessageBody
        //
        headers.setContentType(MediaType.APPLICATION_JSON)
        import scala.collection.JavaConversions._
        //      信息内容
        lazy val contents = List(s"exception:${cause},cause:${cause.getCause},logSource:$logSource,logClass:$logClass,message $message")
        messageBody.setMessageContents(contents)
        //      手机号码列表
        messageBody.setMobiles(mobilelist)
        //      必填，自定义发送者的名字
        messageBody.setSenderName("estuary")
        //      通过resttemlate发送信息
        restTemplate.postForObject(url, messageBody, classOf[String])
      }

      val taskMark = logSource.substring(0, logSource.lastIndexOf("/"))
      lazy val now = System.currentTimeMillis()
      if (now - lastSendTime > TIME_INTERVAL) {
        buildandSendErrorMessage
        lastSendTime = now
      }

    }
  }
}

object EstuaryEventListener {
  def props: Props = Props(new EstuaryEventListener)
}