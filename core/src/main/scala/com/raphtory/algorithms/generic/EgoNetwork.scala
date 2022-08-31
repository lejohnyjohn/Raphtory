package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

/*
Init: find the starting node, set EGONET to true as it's in the graph, propagate to it's neighbours
Iterate:
* */

class EgoNetwork(center_property: String, center_value: String, radius: Int = 2) extends Generic { // todo change default name
  // center_property is e.g. "name": a property of the vector that the algorithm will use to find the central node, and center_value is the value of this property
  final val EGONET = "inEgoNet"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        //vertex.name()
        if (vertex.name(center_property) == center_value) { // todo add alternative constructor that lets you input the ID instead of a property/value pair
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
        iterations = 20, // todo this could be the radius?
        executeMessagedOnly = true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        Row(vertex.name, vertex.getStateOrElse[Boolean](EGONET, false))
      ).filter(r => r.getBool(1))
}

object EgoNetwork {
  def apply(center_property: String, center_value: String, radius: Int = 2) = new EgoNetwork(center_property, center_value, radius)
}
