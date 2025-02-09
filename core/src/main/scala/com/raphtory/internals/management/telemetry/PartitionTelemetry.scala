package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

private[raphtory] object PartitionTelemetry {

  def writerVertexAdditions: Counter =
    Counter.build
      .namespace("partition")
      .name("vertex_adds")
      .help("Total vertex additions")
      .labelNames("partitionID", "graphID")
      .register()

  def writerVertexDeletions: Counter =
    Counter.build
      .namespace("partition")
      .name("vertex_deletes")
      .help("Total vertex deletions")
      .labelNames("partitionID", "graphID")
      .register()

  def writerEdgeAdditions: Counter =
    Counter.build
      .namespace("partition")
      .name("edge_adds")
      .help("Total edge additions")
      .labelNames("partitionID", "graphID")
      .register()

  def writerEdgeDeletions: Counter =
    Counter.build
      .namespace("partition")
      .name("edge_deletes")
      .help("Total edge deletions")
      .labelNames("partitionID", "graphID")
      .register()

}
