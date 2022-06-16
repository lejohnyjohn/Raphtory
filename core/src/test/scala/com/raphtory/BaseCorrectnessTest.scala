package com.raphtory

import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.IdentitySpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.spouts.SequenceSpout

abstract class BaseCorrectnessTest extends BaseRaphtoryAlgoTest[String] {

  override def setGraphBuilder(): GraphBuilder[String] = BasicGraphBuilder()

  def setSpout(): Spout[String] = new IdentitySpout

  private def correctResultsHash(resultsResource: String): String = {
    val source = scala.io.Source.fromResource(resultsResource)
    try resultsHash(source.getLines())
    finally source.close()
  }

  override def beforeAll(): Unit = setup()

  override def afterAll(): Unit = {}

  private def correctResultsHash(rows: IterableOnce[String]): String =
    resultsHash(rows)

  def correctnessTest(
      algorithm: GenericallyApplicable,
      graphResource: String,
      resultsResource: String,
      lastTimestamp: Int
  ): Boolean =
    Raphtory
      .load(ResourceSpout(graphResource), setGraphBuilder())
      .use {
        algorithmPointTest(algorithm, lastTimestamp)
      }
      .map(actual =>
        actual == correctResultsHash(
                resultsResource
        )
      )
      .unsafeRunSync()

  def correctnessTest(
      algorithm: GenericallyApplicable,
      graphEdges: Seq[String],
      results: Seq[String],
      lastTimestamp: Int
  ): Boolean =
    Raphtory
      .load(SequenceSpout(graphEdges: _*), setGraphBuilder())
      .use {
        algorithmPointTest(
                algorithm,
                lastTimestamp
        )
      }
      .map(actual =>
        actual == correctResultsHash(
                results
        )
      )
      .unsafeRunSync()
}
