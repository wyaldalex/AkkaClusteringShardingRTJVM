package part3_clustering

import akka.actor.{Actor, ActorLogging, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory

object ClusterSubscriber {
  def props : Props = Props(new ClusterSubscriber)
}
class ClusterSubscriber extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case MemberJoined(member) =>
      log.info(s"New member in town: ${member.address}")
    case MemberUp(member) if member.hasRole("numberCruncher") =>
      log.info(s"HELLO BROTHER: ${member.address}")
    case MemberUp(member) =>
      log.info(s"Let's say welcome to the newest member: ${member.address}")
    case MemberRemoved(member, previousStatus) =>
      log.info(s"Poor ${member.address}, it was removed from $previousStatus")
    case UnreachableMember(member) =>
      log.info(s"Uh oh, member ${member.address} is unreachable")
    case m: MemberEvent =>
      log.info(s"Another member event: $m")
  }
}


object ClusteringBasics extends App {

  def startCluster(ports: List[Int]) : Unit = ports.foreach { port =>
    val config = ConfigFactory.parseString(
      s"""
         |akka.remote.artery.canonical.port = $port
         |""".stripMargin
    ).withFallback(ConfigFactory.load("part3_clustering/clusteringBasics.conf"))

    val systems = ActorSystem("RTJVMCluster", config)
    systems.actorOf(ClusterSubscriber.props,"clusterSubscriber")

  }
  startCluster(List(2551,2552,0))

}
