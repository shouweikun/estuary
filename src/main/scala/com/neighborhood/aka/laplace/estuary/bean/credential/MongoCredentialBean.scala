package com.neighborhood.aka.laplace.estuary.bean.credential

import scala.beans.BeanProperty

/**
  * Created by john_liu on 2018/4/24.
  */
@BeanProperty
final case class MongoCredentialBean(
                                      val username:String,
                                      val password:String,
                                      val database:String
                                    ) {


}
