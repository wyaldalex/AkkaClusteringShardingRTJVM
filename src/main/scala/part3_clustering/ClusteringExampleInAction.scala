package part3_clustering

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object ClusteringExampleDomain {
  case class ProcessFile(fileName: String)
  case class ProcessLine(line: String)
  case class ProcessLineResult(count: Int)
}

object Master {
  def props : Props = Props(new Master)
}
class Master extends Actor with ActorLogging {

  import ClusteringExampleDomain._

  implicit val timeout = Timeout(3 seconds)
  import context.dispatcher

  val cluster = Cluster(context.system)
  var workers: Map[Address,ActorRef] = Map()
  var pendingRemoval: Map[Address, ActorRef] = Map()

  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = handleClusterEvents.orElse(handleWorkerRegistration)

  def handleClusterEvents: Receive = {
    case MemberUp(member) if member.hasRole("worker") =>
      log.info(s"Member is up: ${member.address}")
      if (pendingRemoval.contains(member.address)) {
        pendingRemoval = pendingRemoval - member.address
      } else {
        val workerSelection = context.actorSelection(s"${member.address}/user/worker")
        workerSelection.resolveOne().map(ref => (member.address, ref)).pipeTo(self)
      }

      //12 min
    case UnreachableMember(member) if member.hasRole("worker") =>
      log.info(s"Uh oh, member ${member.address} is unreachable")
      val workerOption = workers.get(member.address)
      workerOption.foreach { ref =>
        pendingRemoval = pendingRemoval + (member.address -> ref)
      }
    case MemberRemoved(member, previousStatus) =>
      log.info(s"Member ${member.address}, removed after $previousStatus")
      workers = workers - member.address

    case m: MemberEvent =>
      log.info(s"Another member event I don't care about: $m")
  }

  def handleWorkerRegistration: Receive = {
    case pair: (Address, ActorRef) =>
      log.info("Worker successfully registered")
      workers = workers + pair
  }

}
object Worker {
  def props : Props = Props(new Worker)
}
class Worker extends Actor with ActorLogging {
  override def receive: Receive = {
    case _ => log.info("Worker succesfully registered")
  }
}

object SeedNodes extends App {

  def createNode(port: Int, role: String, props: Props, actorName: String) = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.cluster.roles = ["$role"]
         |akka.remote.artery.canonical.port = $port
         |""".stripMargin
    ).withFallback(ConfigFactory.load("part3_clustering/clusteringExample.conf"))

    val system = ActorSystem("RTJVMCluster", config)
    system.actorOf(props, actorName)
  }
  createNode(2551,"master",Master.props,"master")
  createNode(2552,"worker",Master.props,"worker")
  createNode(2553,"worker",Master.props,"worker")

}
