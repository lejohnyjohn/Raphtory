package com.raphtory.examples.lotrTopic

import cats.effect.{ExitCode, IO, IOApp}
import com.raphtory.Raphtory
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object SimpleRunner extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

    FileUtils.curlFile(path, url)

    val source  = FileSpout(path)
    val builder = new LOTRGraphBuilder()
    Raphtory.stream(spout = source, graphBuilder = builder).use { _ =>
      IO.pure(ExitCode.Success)
    }
  }
}
