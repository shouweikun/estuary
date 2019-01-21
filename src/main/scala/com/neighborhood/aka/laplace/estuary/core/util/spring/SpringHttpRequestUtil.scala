package com.neighborhood.aka.laplace.estuary.core.util.spring

import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.web.client.RestTemplate

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by john_liu on 2018/7/25.
  */
object SpringHttpRequestUtil {

  import scala.reflect._

  private lazy val restTemplate = new RestTemplate
  private lazy val log = LoggerFactory.getLogger("SpringHttpRequestUtil")

  def doPost[A, B:ClassTag](url: String, request: A, uriParam: AnyRef*): Try[B] = Try {
    log.info(s"start post $url:$request")


    restTemplate.postForObject(url, request, classTag[B].runtimeClass, uriParam).asInstanceOf[B]
  }

  def doGet[A:ClassTag](url: String, uriParam: Any*): Try[A] = Try {
    log.info(s"start get $url.$uriParam")
    lazy val runtimeClass = classTag[A].runtimeClass
    restTemplate.getForObject(url, runtimeClass, uriParam).asInstanceOf[A]
  }
}
