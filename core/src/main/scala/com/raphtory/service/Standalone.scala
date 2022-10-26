package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object Standalone extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      service <- RaphtoryServiceBuilder.standalone[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()
  }

}
