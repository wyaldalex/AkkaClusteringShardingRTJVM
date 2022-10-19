package part2_remoting

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorSystem, Identify, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object RemoteActors extends App {

  val localSystem = ActorSystem("LocalSystem", ConfigFactory.load("part2_remoting/remoteActors.conf"))
  //val remoteSystem = ActorSystem("RemoteSystem", ConfigFactory.load("part2_remoting/remoteActors.conf").getConfig("remoteSystem"))

  val localSimpleActor = localSystem.actorOf(SimpleActor.props, "localSimpleActor")
  //val remoteSimpleActor = remoteSystem.actorOf(SimpleActor.props, "remoteSimpleActor")

  //localSimpleActor ! "Hey Local!"
  //remoteSimpleActor ! "Whazuuup remote"
  val remoteActorSelection = localSystem.actorSelection("akka://RemoteSystem@localhost:2552/user/remoteSimpleActor")
  remoteActorSelection ! "Whazuuup remote"

  //Method 2
  import localSystem.dispatcher
  implicit val timeout = Timeout(2.seconds)
  val remoteActorRefFuture = remoteActorSelection.resolveOne()
  remoteActorRefFuture.onComplete {
    case Success(actorRef) => actorRef ! "Ive resolved you in a future"
    case Failure(exception)  => println("Failed to retrieve actor reference")
  }

  //Method 3
  object ActorResolver{
    def props : Props = Props(new ActorResolver)
  }
  class ActorResolver extends Actor with ActorLogging {
    override def preStart(): Unit = {
      val selection = context.actorSelection("akka://RemoteSystem@localhost:2552/user/remoteSimpleActor")
      selection ! Identify(42)
      log.info("Does it ever get executed " + selection.toString())
    }
    override def receive: Receive = {
      case ActorIdentity(42, Some(actorRef)) =>
        actorRef !  "Thank you for identifying yourself"

    }

  }
  localSystem.actorOf(ActorResolver.props, "actorResolver")
}


object RemoteSystemApp extends App {

  val remoteSystem = ActorSystem("RemoteSystem", ConfigFactory.load("part2_remoting/remoteActors.conf").getConfig("remoteSystem"))
  val remoteSimpleActor = remoteSystem.actorOf(SimpleActor.props, "remoteSimpleActor")

}
