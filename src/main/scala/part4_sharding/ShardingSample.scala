package part4_sharding

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.typesafe.config.ConfigFactory

import java.util.{Date, UUID}
import scala.util.Random

case class OysterCard(id: String, amount: Double)
case class EntryAttempt(oysterCard: OysterCard, date: Date)
case object EntryAccepted
case class EntryRejected(reason: String)

object Turnstile {
  def props(validator: ActorRef) : Props = Props(new Turnstile(validator))
}
class Turnstile(validator: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case o: OysterCard => validator ! EntryAttempt(o, new Date())
    case EntryAccepted => log.info("GREEN: please pass")
    case EntryRejected(reason) => log.info(s"RED: $reason")
  }

}

class OysterCardValidator extends Actor with ActorLogging {
  /*
  Stores an enormous amount of data, think one of those persistent actors
  that stored the maps with the state
   */

  override def preStart(): Unit = {
    super.preStart()
    log.info("Validator Starting")
  }
  override def receive: Receive = {
    case EntryAttempt(card@OysterCard(id,amount), _) =>
      log.info(s"Validating $card")
      if (amount > 2.5) sender() ! EntryAccepted
      else sender() ! EntryRejected(s"[$id] not enough funds, please top up")

  }
}

////////////////////////////////////
////Sharding settings//////////////
////////////////////////////////////

object TurnstileSettings {
  val numberOfShards = 10 // use 10x number of nodes in your cluster
  val numberOfEntities = 100 //10x number of shards

  //this help to map the corresponding message to a respective entity
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case attempt@EntryAttempt(OysterCard(cardId,_), _) =>
      val entityId = cardId.hashCode.abs % numberOfEntities
      (entityId.toString, attempt)
  }

  //this help to map the corresponding message to a respective shard
  val extractShardId: ShardRegion.ExtractShardId = {
    case EntryAttempt(OysterCard(cardId,_), _) =>
      val shardId = cardId.hashCode.abs % numberOfShards
      shardId.toString
    case ShardRegion.StartEntity(entityId) =>
      (entityId.toLong % numberOfShards).toString
  }
}
/*
 There must be NO two messages M1 and M2 for which
 extractEntityId(M1) == extractEntity(M2) and extractShardId(M1) != extractShardId(M2)

 M1 -> E37, S9
 M2 -> E37, S10
 */

////////////////////////////////////
////Cluster Nodes///////////////////
////////////////////////////////////
class TubeStation(port: Int, numberOfTunstiles: Int) extends App {
  val config = ConfigFactory.parseString(
    s"""
       |akka.remote.artery.canonical.port = $port
  """.stripMargin)
    .withFallback(ConfigFactory.load("part4_sharding/shardingSampleConfig.conf"))

  val system = ActorSystem("RTJVMCluster", config)

  // Setting up Cluster Sharding
  val validatorShardRegionRef: ActorRef = ClusterSharding(system).start(
    typeName = "OysterCardValidator",
    entityProps = Props[OysterCardValidator],
    settings = ClusterShardingSettings(system).withRememberEntities(true),
    extractEntityId = TurnstileSettings.extractEntityId,
    extractShardId = TurnstileSettings.extractShardId
  )

  val turnstiles = (1 to numberOfTunstiles).map(_ => system.actorOf(Turnstile.props(validatorShardRegionRef)))

  Thread.sleep(10000)
  for(_ <- 1 to 1000) {
    val randomTurnstileIndex = Random.nextInt(numberOfTunstiles)
    val randomTurnstile = turnstiles(randomTurnstileIndex)

    randomTurnstile ! OysterCard(UUID.randomUUID().toString, Random.nextDouble()*10)
    Thread.sleep(200)
  }
}

object PiccadillyCircus extends TubeStation(2551,10)
object Westminster extends TubeStation(2561,5)
object CharingCross extends TubeStation(2571,15)