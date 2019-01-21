package com.neighborhood.aka.laplace.estuary.core.util.message

import com.neighborhood.aka.laplace.estuary.core.util.spring.SpringHttpRequestUtil

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by john_liu on 2018/7/25.
  */

object MessageSender {
  /**
    *
    * @param contents  发送内容
    * @param mobileList 手机号
    * @param senderName 发送者
    * @param url 调用的url
    * @tparam A 返回值类型
    * @return
    */
  def sendMessage[A:ClassTag](contents: List[String], mobileList: List[String], senderName: String)(url: String): Try[A] = {
    import scala.collection.JavaConversions._
    lazy val message = MessageBody.buildMessage(contents, mobileList, senderName)
    SpringHttpRequestUtil.doPost[MessageBody, A](url, message)
  }

  /**
    *  发送短信，返回值String
    * @param contents
    * @param mobileList
    * @param senderName
    * @param url
    * @return
    */
  def sendMessageReturnWithinString(contents: List[String], mobileList: List[String], senderName: String = "estuary")(url: String): Try[String] = sendMessage[String](contents, mobileList, senderName)(url)


}
