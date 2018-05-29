package com.neighborhood.aka.laplace.estuary.core.`implicit`

import com.neighborhood.aka.laplace.estuary.core.trans.{RegTransPlugin, RegTransformation}

/**
  * Created by john_liu on 2018/5/28.
  */
object temp extends App {
  val regRules = List(RegTransformation("1", "2"), RegTransformation("23", "34"))
  val transPlugin = RegTransPlugin(regRules)

  implicit class trans(str: String) {
    def transfer = RegTransPlugin.transfer(str, transPlugin)
  }

  println("1".transfer)
}
