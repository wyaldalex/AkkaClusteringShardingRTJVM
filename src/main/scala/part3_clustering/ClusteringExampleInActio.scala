package part3_clustering

import akka.actor.{Actor, ActorLogging, ActorRef, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.pattern.pipe
import akka.util.Timeout

import scala.concurrent.duration._

object ClusteringExampleDomain {
  case class ProcessFile(fileName: String)
  case class ProcessLine(line: String)
  case class ProcessLineResult(count: Int)
}

class Master extends Actor with ActorLogging {

  import ClusteringExampleDomain._

  implicit val timeout = Timeout(3 seconds)
  import context.dispatcher

  val cluster = Cluster(context.system)
  val workers: Map[Address,ActorRef] = Map()
  val pendingRemoval: Map[Address, ActorRef] = Map()

  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = handleClusterEvents
  def handleClusterEvents: Receive = {
    case MemberUp(member) =>
      log.info(s"Member is up: ${member.address}")
      val workerSelection = context.actorSelection(s"${member.address}")
      workerSelection.resolveOne().map(ref => (member.address, ref)).pipeTo(self)
      //12 min

    case MemberRemoved(member, previousStatus) =>
      log.info(s"Poor ${member.address}, it was removed from $previousStatus")
    case UnreachableMember(member) =>
      log.info(s"Uh oh, member ${member.address} is unreachable")
    case m: MemberEvent =>
      log.info(s"Another member event: $m")
  }

}
