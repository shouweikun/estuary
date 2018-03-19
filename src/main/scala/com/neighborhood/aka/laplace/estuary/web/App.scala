package com.neighborhood.aka.laplace.estuary.web

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.web.servlet.ServletComponentScan

/**
  * Created by john_liu on 2018/3/11.
  */
@SpringBootApplication
@ServletComponentScan(basePackages = Array("com.finup"))
class App {

}
object App {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[App])
  }

}
