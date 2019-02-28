package com.neighborhood.aka.laplace.estuary.mongo.source

import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MongoSourceBean

/**
  * Created by john_liu on 2019/2/27.
  *
  * @author neighborhood.aka.laplace
  */
final case class MongoBeanImp(
                               override val authMechanism: String,
                               override val mongoCredentials: List[MongoCredentialBean],
                               override val hosts: List[String],

                               override val port: Int)(
override val concernedNs: Array[String] = Array.empty,
override val  ignoredNs: Array[String] = Array.empty
                             ) extends MongoSourceBean
