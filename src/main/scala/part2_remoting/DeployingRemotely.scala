package part2_remoting

import akka.actor.{Actor, ActorLogging, ActorSystem, Address, AddressFromURIString, Deploy, PoisonPill, Props, Terminated}
import akka.remote.RemoteScope
import akka.routing.FromConfig
import com.typesafe.config.ConfigFactory

object DeployingRemotely extends App {
  val system = ActorSystem("LocalActorSystem", ConfigFactory.load("part2_remoting/deployingActorsRemotely.conf").getConfig("localApp"))
  val simpleActor = system.actorOf(SimpleActor.props, "remoteActor") // /user/remoteActor
  // This goes into the remote actor
  simpleActor ! "hello, remote actor!"

}

object DeployedRemotelyApp extends App {
  val system = ActorSystem("RemoteActorSystem", ConfigFactory.load("part2_remoting/deployingActorsRemotely.conf").getConfig("remoteApp"))
}
