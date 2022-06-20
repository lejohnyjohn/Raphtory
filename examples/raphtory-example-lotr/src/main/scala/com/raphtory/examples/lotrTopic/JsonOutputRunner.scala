package com.raphtory.examples.lotrTopic

import cats.effect.ExitCode
import cats.effect.IO
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.NodeInformation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.formats.JsonFormat
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object JsonOutputRunner extends App {
  val path    = "/tmp/lotr.csv"
  val url     = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)
  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()

  val graph = Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
    IO.blocking {

      val filepath = "/tmp/gsonLotrOutput"
      val output   = FileSink(filepath, format = JsonFormat())

      graph
        .at(32674)
        .past()
        .execute(NodeInformation(initialID = 5415127257870295999L))
        .writeTo(output)
        .waitForJob()
      ExitCode.Success
    }
  }
}
