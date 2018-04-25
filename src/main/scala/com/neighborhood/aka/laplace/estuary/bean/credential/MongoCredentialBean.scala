package com.neighborhood.aka.laplace.estuary.bean.credential

import scala.beans.BeanProperty

/**
  * Created by john_liu on 2018/4/24.
  */
@BeanProperty
class MongoCredentialBean(
                            val username: Option[String] = None,
                            val password: Option[String] = None,
                            val database: Option[String] = None
                         ) {


}
