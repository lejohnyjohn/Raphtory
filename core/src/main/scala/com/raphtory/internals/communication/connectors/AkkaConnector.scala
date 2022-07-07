package com.raphtory.internals.communication.connectors

import akka.actor.Cancellable
import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import cats.effect.Resource
import cats.effect.Sync
import com.raphtory.internals.communication.BroadcastTopic
import com.raphtory.internals.communication.CancelableListener
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.Connector
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.ExclusiveTopic
import com.raphtory.internals.communication.Topic
import com.raphtory.internals.communication.WorkPullTopic
import com.raphtory.internals.serialisers.KryoSerialiser
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.CompletableFuture
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.Try

sealed trait ReliableProtocolCommand
sealed trait SenderCommand                    extends ReliableProtocolCommand
case class EnsureDelivery(count: Long)        extends SenderCommand
case class Ack(count: Long, recipientId: Int) extends SenderCommand
case class SendRequest(message: Array[Byte])  extends SenderCommand

sealed trait ReceiverCommand  extends ReliableProtocolCommand
private case object StopActor extends ReceiverCommand

case class Ordered(
    count: Long,
    message: Array[Byte],
    senderId: UUID,
    recipientId: Int,
    replyTo: ActorRef[SenderCommand]
) extends ReceiverCommand

private[raphtory] class AkkaConnector(actorSystem: ActorSystem[SpawnProtocol.Command]) extends Connector {
  private val logger: Logger                              = Logger(LoggerFactory.getLogger(this.getClass))
  private val kryo                                        = KryoSerialiser()
  private val akkaReceptionistRegisteringTimeout: Timeout = 1.seconds
  private val akkaSpawnerTimeout: Timeout                 = 1.seconds
  private val akkaReceptionistFindingTimeout: Timeout     = 1.seconds
  private val selfResolutionTimeout: Duration             = 1.seconds
  private val ackTimeout: Timeout                         = 1.seconds

  class ReliableSender(
      recipients: List[ActorRef[ReceiverCommand]],
      context: ActorContext[SenderCommand],
      topicName: String
  ) extends AbstractBehavior[SenderCommand](context) {
    private val ackTimeout                = 1.seconds
    private val recipientsWithIndexes     = recipients zipWithIndex
    private val buffer                    = mutable.Map[Long, Array[Byte]]()
    private val numberOfMessagesDelivered = mutable.ArraySeq.fill(recipients.size)(0L)
    private val senderId                  = UUID.randomUUID()

    private var numberOfMessagesSent           = 0L
    private var numberOfMessagesDeliveredToAll = numberOfMessagesDelivered.min
    private var recheck: Option[Cancellable]   = None

    override def onMessage(msg: SenderCommand): Behavior[SenderCommand] = {
      msg match {
        case SendRequest(message)        =>
          numberOfMessagesSent += 1
          buffer.addOne(numberOfMessagesSent, message)
          recipientsWithIndexes foreach {
            case (recipient, index) =>
              recipient ! Ordered(numberOfMessagesSent, message, senderId, index, context.self)
          }
          scheduleRecheck(context, numberOfMessagesSent)
        case Ack(count, recipientId)     =>
          logger.info(s"received ACK from recipient $recipientId for count $count and topic $topicName")
          if (numberOfMessagesDelivered(recipientId) < count)
            numberOfMessagesDelivered(recipientId) = count
          val numberOfMessagesDeliveredToAllUpdated = numberOfMessagesDelivered.min
          val messagesToDrop                        = (numberOfMessagesDeliveredToAll + 1) to numberOfMessagesDeliveredToAllUpdated
          buffer.subtractAll(messagesToDrop)
          numberOfMessagesDeliveredToAll = numberOfMessagesDeliveredToAllUpdated
        case EnsureDelivery(globalCount) =>
          if (numberOfMessagesDeliveredToAll < globalCount) {
            logger.info(
                    s"Oldest pending $numberOfMessagesDeliveredToAll is lower or equal to the expected count $globalCount after one second for topic $topicName"
            )
            val remainingIndexes = numberOfMessagesDelivered.zipWithIndex collect {
              case (count, id) if count < globalCount => id
            }
            remainingIndexes foreach { index =>
              val messageToSend = numberOfMessagesDeliveredToAll + 1
              recipients(index) ! Ordered(messageToSend, buffer(messageToSend), senderId, index, context.self)
            }
            scheduleRecheck(context, numberOfMessagesSent)
          }
          else
            logger.info(s"Ensured the delivery of $globalCount messages in topic $topicName")
      }
      this
    }

    private def scheduleRecheck(context: ActorContext[SenderCommand], numberOfMessages: Long): Unit = {
      recheck foreach (_.cancel())
      recheck = Some(context.scheduleOnce(ackTimeout, context.self, EnsureDelivery(numberOfMessages)))
    }
  }

  object ReliableSender {

    def apply[T](actorRefs: Future[Set[ActorRef[ReceiverCommand]]], topicName: String): Behavior[SenderCommand] =
      Behaviors.setup[SenderCommand] { context =>
        val endPointResolutionTimeout: Duration = 60.seconds
        val recipients = {
          try Await.result(actorRefs, endPointResolutionTimeout).toList
          catch {
            case e: concurrent.TimeoutException =>
              val msg =
                s"Impossible to connect to topic '$topicName' through Akka. Maybe some components weren't successfully deployed"
              logger.error(msg)
              throw new Exception(msg, e)
          }
        }
        logger.info(s"Recipients obtained for topic $topicName")
        new ReliableSender(recipients, context, topicName)
      }
  }

  class AkkaEndPoint[T](actorRefs: Future[Set[ActorRef[ReceiverCommand]]], topicName: String) extends EndPoint[T] {
    private val reliableSender = spawnBehaviorAndWait(ReliableSender[T](actorRefs, topicName), "")

    override def sendAsync(message: T): Unit           = reliableSender ! SendRequest(kryo.serialise(message))
    override def close(): Unit = {}
    override def flushAsync(): CompletableFuture[Void] = CompletableFuture.completedFuture(null)
    override def closeWithMessage(message: T): Unit    = sendAsync(message)
  }

  override def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T] = {
    val (serviceKey, numActors) = topic match {
      case topic: ExclusiveTopic[T] => (getServiceKey(topic), 1)
      case topic: BroadcastTopic[T] => (getServiceKey(topic), topic.numListeners)
      case _: WorkPullTopic[T]      =>
        throw new Exception("work pull topics are not supported by Akka connector")
    }
    val topicName               = s"${topic.id}/${topic.subTopic}"
    implicit val ec             = actorSystem.executionContext
    new AkkaEndPoint(Future(queryServiceKeyActors(serviceKey, numActors)), topicName)
  }

  class OrderedReceiver[T](messageHandler: T => Unit, context: ActorContext[ReceiverCommand], componentId: String)
          extends AbstractBehavior[ReceiverCommand](context) {

    private val expectedCounts = mutable.Map[UUID, Long]()

    override def onMessage(msg: ReceiverCommand): Behavior[ReceiverCommand] = {
      logger.trace(s"Processing message by component $componentId")
      try msg match {
        case StopActor                                               =>
          logger.debug(s"Closing akka listener for component $componentId")
          Behaviors.stopped[ReceiverCommand]
        case Ordered(count, message, senderId, recipientId, replyTo) =>
          logger.info(s"message '$message' received by component $componentId from $replyTo")
          val expectedCount = expectedCounts.getOrElse(senderId, 1)
          if (count == expectedCount) {
            messageHandler.apply(kryo.deserialise(message))
            logger.info(s"message handled by component $componentId from $replyTo. Sending ACK")
            replyTo ! Ack(count, recipientId)
            expectedCounts(senderId) = count + 1
          }
          else
            logger.info(
                    s"message received by component $componentId from $replyTo had unexpected id $count (should have been $expectedCount)"
            )
          this
        case msg                                                     =>
          logger.error(s"Unrecognized message $msg")
          this
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error(s"Component $componentId: Failed to handle message. ${e.getMessage}")
          throw e
      }
    }
  }

  object OrderedReceiver {

    def apply[T](
        componentId: String,
        messageHandler: T => Unit,
        topics: Seq[CanonicalTopic[T]]
    ): Behavior[ReceiverCommand] =
      Behaviors.setup[ReceiverCommand] { context =>
        implicit val timeout: Timeout     = akkaReceptionistRegisteringTimeout
        implicit val scheduler: Scheduler = context.system.scheduler
        topics foreach { topic =>
          context.system.receptionist ! Receptionist.Register(getServiceKey(topic), context.self)
        }
        new OrderedReceiver(messageHandler, context, componentId)
      }
  }

  override def register[T](
      componentId: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = {
    val behavior = OrderedReceiver(componentId, messageHandler, topics)
    new CancelableListener {
      var futureSelf: Option[Future[ActorRef[ReceiverCommand]]] = None
      override def start(): Unit                                = futureSelf = Some(spawnBehavior(behavior, componentId))

      override def close(): Unit =
        futureSelf.foreach { futureSelf =>
          val selfResolutionTimeout: Duration = 10.seconds
          Await.result(futureSelf, selfResolutionTimeout) ! StopActor
        }
    }
  }

  @tailrec
  private def queryServiceKeyActors(
      serviceKey: ServiceKey[ReceiverCommand],
      numActors: Int
  ): Set[ActorRef[ReceiverCommand]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    implicit val timeout: Timeout     = akkaReceptionistFindingTimeout
    val futureListing                 = actorSystem.receptionist ? Receptionist.Find(serviceKey)
    val listing                       = Await.result(futureListing, 1.seconds)
    val instances                     = listing.serviceInstances(serviceKey)

    if (instances.size == numActors)
      instances
    else if (instances.size > numActors) {
      val msg =
        s"${instances.size} instances for service key '${serviceKey.id}', but only $numActors expected"
      throw new Exception(msg)
    }
    else
      queryServiceKeyActors(serviceKey, numActors)
  }

  def spawnBehavior[T](behavior: Behavior[T], name: String): Future[ActorRef[T]] = {
    implicit val timeout: Timeout     = akkaSpawnerTimeout
    implicit val scheduler: Scheduler = actorSystem.scheduler
    val spawnRequestBuilder           = (ref: ActorRef[ActorRef[T]]) => SpawnProtocol.Spawn(behavior, name, Props.empty, ref)
    actorSystem ? spawnRequestBuilder
  }

  def spawnBehaviorAndWait[T](behavior: Behavior[T], name: String): ActorRef[T] = {
    val futureSelf = spawnBehavior(behavior, name)
    Await.result(futureSelf, selfResolutionTimeout)
  }

  private def getServiceKey[T](topic: Topic[T]) =
    ServiceKey[ReceiverCommand](s"${topic.id}-${topic.subTopic}")

  override def shutdown(): Unit = {}
}

object AkkaConnector {
  sealed trait Mode
  case object StandaloneMode extends Mode
  case object ClientMode     extends Mode
  case object SeedMode       extends Mode

  private val systemName = "spawner"

  def apply[IO[_]: Sync](mode: Mode, config: Config): Resource[IO, AkkaConnector] =
    Resource
      .make(Sync[IO].delay(buildActorSystem(mode, config)))(system => Sync[IO].delay(system.terminate()))
      .map(new AkkaConnector(_))

  private def buildActorSystem(mode: Mode, config: Config) =
    mode match {
      case AkkaConnector.StandaloneMode => ActorSystem(SpawnProtocol(), systemName)
      case _                            =>
        val seed                = mode match {
          case AkkaConnector.SeedMode   => true
          case AkkaConnector.ClientMode => false
        }
        val providedSeedAddress = config.getString("raphtory.query.address")
        val seedAddress         =
          Try(InetAddress.getByName(providedSeedAddress).getHostAddress)
            .getOrElse(providedSeedAddress)
        ActorSystem(SpawnProtocol(), systemName, akkaConfig(seedAddress, seed, config))
    }

  private def akkaConfig(seedAddress: String, seed: Boolean, config: Config) = {
    val localCanonicalPort = if (seed) advertisedPort(config) else "0"
    val localBindPort      = if (seed) bindPort(config) else "\"\"" // else, it uses canonical port instead
    val hostname           = if (seed) seedAddress else "<getHostAddress>"
    ConfigFactory.parseString(s"""
      akka.cluster.downing-provider-class="akka.cluster.sbr.SplitBrainResolverProvider"
      akka.remote.artery.canonical.hostname="$hostname"
      akka.remote.artery.canonical.port=$localCanonicalPort
      akka.remote.artery.bind.port=$localBindPort
      akka.actor.provider=cluster
      akka.cluster.seed-nodes=["akka://$systemName@$seedAddress:${advertisedPort(config)}"]
    """)
  }

  private def advertisedPort(config: Config) = config.getString("raphtory.akka.port")
  private def bindPort(config: Config)       = config.getString("raphtory.akka.bindPort")
}
