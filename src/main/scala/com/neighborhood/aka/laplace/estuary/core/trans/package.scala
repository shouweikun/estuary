package com.neighborhood.aka.laplace.estuary.core

import com.neighborhood.aka.laplace.estuary.core.plugin.Plugin

/**
  * Created by john_liu on 2018/5/26.
  */
package object trans {

  sealed trait TransPlugin extends Plugin

  case class RegTransformation(reg: String, target: String)

  case class RegTransPlugin(regs: List[RegTransformation]) {
    lazy val compiledRegMap = regs.map(x => (x.reg.r -> x.target))

    def transfer(string: String): String = {
      lazy val re = compiledRegMap
        .withFilter(
          _._1
            .findFirstIn(string)
            .isDefined
        )
        .map(_._2)

      if (re.isEmpty) string else re(0)
    }

  }
  case object RegTransPlugin{
    def transfer(str:String,regTransPlugin: RegTransPlugin):String = {
      regTransPlugin.transfer(str)
    }
  }

}
