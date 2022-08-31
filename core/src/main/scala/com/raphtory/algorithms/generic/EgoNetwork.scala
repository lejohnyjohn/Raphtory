package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

/*

* */

class EgoNetwork(name: String = "Gandalf", radius: Int = 2) extends Generic {

  final val EGONET = "in_ego_net"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.getPropertyOrElse("name", "") == name) {
          vertex.messageAllNeighbours(0)
          vertex.setState(EGONET, true)
        } // else -1?
      }
      .iterate(
        { vertex =>
          // todo if already in, don't need to show?
          // take min of received queue and add one
          // if new curr radius < total radius, propagate
          vertex.setState(EGONET, true)
          val local_radius = vertex.messageQueue[Int].min + 1
          if (local_radius < radius) { // not done yet, propagate
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
