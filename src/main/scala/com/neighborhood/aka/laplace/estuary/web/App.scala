package com.neighborhood.aka.laplace.estuary.web

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.boot.web.servlet.ServletComponentScan

import org.springframework.boot.autoconfigure.dao.PersistenceExceptionTranslationAutoConfiguration
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration

/**
  * Created by john_liu on 2018/3/11.
  */
@EnableAutoConfiguration(exclude = Array(
  classOf[MongoAutoConfiguration],
  classOf[MongoDataAutoConfiguration],
  classOf[PersistenceExceptionTranslationAutoConfiguration]))
@SpringBootApplication
@ServletComponentScan(basePackages = Array("com.neighborhood.aka.laplace.web"))
class App {

}


object App {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[App])
  }

}
