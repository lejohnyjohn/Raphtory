package com.raphtory.aws

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective

class FilteredOutDegree extends NodeList(Seq("filteredOutDegree")) {
  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step{ vertex =>
      vertex.setState("outDegree", vertex.outDegree)
    }
//      .step{ vertex =>
//      if (vertex.outDegree > 100)
//        vertex.setState("filteredOutDegree", vertex.outDegree)
//    }
}

object FilteredOutDegree {
  def apply() = new FilteredOutDegree()
}
