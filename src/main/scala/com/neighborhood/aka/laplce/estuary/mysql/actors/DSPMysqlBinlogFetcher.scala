package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.Actor
import com.alibaba.otter.canal.protocol.position.EntryPosition
import org.apache.commons.lang.StringUtils

/**
  * Created by john_liu on 2018/2/5.
  */
class DSPMysqlBinlogFetcher extends Actor {
  var entryPosition: Option[EntryPosition] = None

  //offline
  override def receive: Receive = {
    case ep: EntryPosition => {
      entryPosition = Option(ep)
      if (entryPosition.isDefined) {
         context.become(online)
         self ! "start"
      }
    }
    case FetcherMessage(msg) => {

    }
    case EventParserMessage(msg) => {

    }
  }

  def online: Receive = {
    case FetcherMessage(msg) => {
      msg match {
        case "start" => {
           val startPosition = entryPosition.get
          if (StringUtils.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty){}
        }
      }
    }
    case EventParserMessage(msg) => {

    }
  }
}
