package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.communication.Connector
import com.raphtory.internals.communication.TopicRepository
import com.typesafe.config.Config

private[raphtory] object PulsarAkkaTopicRepository {

  def apply[IO[_]: Async](config: Config): Resource[IO, TopicRepository] =
    for {
      akkaConnector   <- AkkaConnector[IO](AkkaConnector.StandaloneMode, config)
      pulsarConnector <- PulsarConnector[IO](config)
    } yield new TopicRepository(pulsarConnector, config, Array(akkaConnector, pulsarConnector)) {
      override def jobOperationsConnector: Connector      = akkaConnector
      override def jobStatusConnector: Connector          = akkaConnector
      override def queryPrepConnector: Connector          = akkaConnector
      override def completedQueriesConnector: Connector   = akkaConnector
      override def watermarkConnector: Connector          = akkaConnector
      override def rechecksConnector: Connector           = akkaConnector
      override def queryTrackConnector: Connector         = akkaConnector
      override def submissionsConnector: Connector        = akkaConnector
      override def vertexMessagesSyncConnector: Connector = akkaConnector
    }
}
