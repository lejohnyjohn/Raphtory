package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.Coreness
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class CorenessTest extends BaseCorrectnessTest {
  withGraph.test("Coreness Test, start = 1, end = 5") { graph =>
    correctnessTest(
      TestQuery(Coreness(1, 5), 29),
      "KCore/CorenessResults.csv",
      graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("KCore/CorenessInput.csv"))
}
