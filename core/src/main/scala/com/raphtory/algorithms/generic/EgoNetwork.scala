package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

/*
Init: find the starting node, set EGONET to true as it's in the graph, propagate to it's neighbours
Iterate:
* */

class EgoNetwork(name: String = "Gandalf", radius: Int = 2) extends Generic { // todo change default name

  final val EGONET = "in_ego_net"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.getPropertyOrElse("name", "") == name) {
          vertex.messageAllNeighbours(0)
          vertex.setState(EGONET, true)
        }
      }
      .iterate(
        { vertex =>
          // todo if already in, don't need to show?
          vertex.setState(EGONET, true)
          val local_radius = vertex.messageQueue[Int].min + 1
          if (local_radius < radius) { // this isn't the outermost layer, propagate
            vertex.messageAllNeighbours(local_radius)
          }
        },
        iterations = 20, // todo could be radius?
        executeMessagedOnly = true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        Row(vertex.name, vertex.getStateOrElse[Boolean](EGONET, false))
      ).filter(r => r.getBool(1))
}

object EgoNetwork {
  def apply(name: String = "Gandalf", radius: Int = 2) = new EgoNetwork(name, radius)
}
