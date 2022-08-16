package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.Connector
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.connectors.ArrowFlightConnector
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.communication.repositories.ArrowFlightRepository.signatureRegistry
import com.raphtory.internals.management.arrow.ArrowFlightHostAddressProvider
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import com.typesafe.config.Config

private[raphtory] object LocalTopicRepository {

  def apply[IO[_]: Async](
      config: Config,
      addressProvider: ArrowFlightHostAddressProvider
  ): Resource[IO, TopicRepository] =
    config.getString("raphtory.communication.control") match {
      case "auto" | "akka" =>
        for {
          pulsarConnector     <- PulsarConnector[IO](config)
          akkaConnector       <- AkkaConnector[IO](AkkaConnector.StandaloneMode, config)
          arrowFlightConnector = new ArrowFlightConnector(config, signatureRegistry, addressProvider)
        } yield new TopicRepository(akkaConnector, arrowFlightConnector, pulsarConnector, config)
      case "pulsar"        => PulsarConnector[IO](config).map(connector => TopicRepository(connector, config))
    }

}
