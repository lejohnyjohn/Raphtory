package com.raphtory.aws

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

class FilteredOutDegree extends NodeList(Seq("filteredOutDegree")) {

  override def apply(graph: GraphPerspective): graph.Graph = OutDegree(graph)

  override def tabularise(graph: GraphPerspective): Table =
    graph.explodeSelect { vertex =>
       vertex.getState("filteredOutDegree")
    }
}

object FilteredOutDegree {
  def apply() = new FilteredOutDegree()
}
