package com.neighborhood.aka.laplce.estuary.core.akka

import akka.actor.ActorSystem

/**
  * Created by john_liu on 2018/2/1.
  */
trait ActorSystem {
 val system = ActorSystem("Estuary")
}
