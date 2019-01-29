package com.neighborhood.aka.laplace.estuary.mysql.task

/**
  * Created by john_liu on 2019/1/21.
  */
final case class SdaBean(
                         val tableMappingRule: Map[String, String],
                         val  encryptField: Map[String, Set[String]]
                        )
