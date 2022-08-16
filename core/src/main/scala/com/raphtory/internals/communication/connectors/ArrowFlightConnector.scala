package com.raphtory.internals.communication.connectors

import com.raphtory.arrowmessaging._
import com.raphtory.internals.communication.CancelableListener
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.Connector
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ArrowFlightHostAddressProvider
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.arrow.memory._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.beans.BeanProperty

case class ArrowFlightHostAddress(interface: String, port: Int)

case class ServiceDetail(@BeanProperty var partitionId: Int) {
  def this() = this(0)
}

class ArrowFlightConnector(
    config: Config,
    signatureRegistry: ArrowFlightMessageSignatureRegistry,
    addressProvider: ArrowFlightHostAddressProvider
) extends ZookeeperConnector
        with Connector {

  private val logger               = Logger(LoggerFactory.getLogger(this.getClass))
  private val allocator            = new RootAllocator
  private val flightBatchSize      = config.getInt("raphtory.arrow.flight.batchSize")
  private val readBusyWait         = config.getInt("raphtory.arrow.flight.readBusyWait")
  private val deploymentId: String = config.getString("raphtory.deploy.id")

  // Endpoint is supposed to encapsulate a writer and knows how to send the message to the flight server
  case class ArrowFlightEndPoint[T](writer: ArrowFlightWriter) extends EndPoint[T] {

    var counter = new AtomicInteger(0)

    override def sendAsync(message: T): Unit = {

      def sendMsg[R](msg: R)(implicit endPoint: String): Unit =
        writer.synchronized {
          counter.incrementAndGet()
          writer.addToBatch(msg)
          if (counter.get() % flightBatchSize == 0)
            writer.sendBatch()
        }

      message match {
        case msg: VertexMessage[_, _]          => sendMsg(msg)(msg.provider.endpoint)
        case msg: VertexAdd                    => sendMsg(msg)(msg.provider.endpoint)
        case msg: VertexDelete                 => sendMsg(msg)(msg.provider.endpoint)
        case msg: EdgeAdd                      => sendMsg(msg)(msg.provider.endpoint)
        case msg: EdgeDelete                   => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncNewEdgeAdd               => sendMsg(msg)(msg.provider.endpoint)
        case msg: BatchAddRemoteEdge           => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncExistingEdgeAdd          => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncExistingEdgeRemoval      => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncNewEdgeRemoval           => sendMsg(msg)(msg.provider.endpoint)
        case msg: OutboundEdgeRemovalViaVertex => sendMsg(msg)(msg.provider.endpoint)
        case msg: InboundEdgeRemovalViaVertex  => sendMsg(msg)(msg.provider.endpoint)
        case msg: SyncExistingRemovals         => sendMsg(msg)(msg.provider.endpoint)
        case msg: EdgeSyncAck                  => sendMsg(msg)(msg.provider.endpoint)
        case msg: VertexRemoveSyncAck          => sendMsg(msg)(msg.provider.endpoint)
        case _                                 => logger.error("VertexMessage or GraphAlteration expected")
      }
    }

    override def flushAsync(): CompletableFuture[Void] =
      CompletableFuture.completedFuture {
        writer.synchronized {
          if (counter.get() > 0) {
            writer.sendBatch()
            writer.completeSend()
            counter.set(0)
          }
          null
        }
      }

    override def close(): Unit = writer.close()

    override def closeWithMessage(message: T): Unit = {
      sendAsync(message)
      flushAsync()
      close()
    }
  }

  // Starts flight server and reader
  override def register[T](
      partitionId: Int,
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = {

    val (server, reader) = addressProvider.startAndPublishAddress(partitionId, messageHandler)

    new CancelableListener {
      override def start(): Unit = Future(reader.readMessages(readBusyWait))

      override def close(): Unit = {
        println("hello")
        server.close()
        reader.close()
        println("hello2")
      }
    }
  }

  // Starts writer and registers message schema and message handler to a given endpoint
  override def endPoint[T](srcParId: Int, topic: CanonicalTopic[T]): EndPoint[T] = {
    val addresses                               = addressProvider.getAddressAcrossPartitions
    val partitionId                             = topic.subTopic.split("-").last.toInt
    val ArrowFlightHostAddress(interface, port) = addresses(partitionId)

    ArrowFlightEndPoint(ArrowFlightWriter(interface, port, srcParId, allocator, signatureRegistry))
  }

  override def shutdown(): Unit = {}

}
