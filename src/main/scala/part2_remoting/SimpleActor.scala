package part2_remoting

import akka.actor.{Actor, ActorLogging, Props}

object SimpleActor {
  def props : Props = Props(new SimpleActor)
}
class SimpleActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case m => log.info(s"Received $m from ${sender()}")
  }
}
