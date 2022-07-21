package com.raphtory.aws

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective

object OutDegree extends Generic {
  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step{ vertex =>
      val outDegree = vertex.outDegree
      if (outDegree > 100.0)
        vertex.setState("filteredOutDegree", vertex.outDegree)
    }
}
