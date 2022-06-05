package com.raphtory.util

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.util.Random

/** Extended sampling methods for `scala.util.Random`.
  * To make these methods available to instances of `scala.util.Random`,
  * Usage:
  * {{{
  * import com.raphtory.algorithms.utils.Sampling._
  * }}}
  */
object Sampling {

  implicit class WeightedSampling(val random: Random) extends AnyVal {

    /** Weighted random sampling. Returns integer `i` with probability proportional to `weights(i)`.
      * @note This implementation uses binary search to sample the index.
      */
    def sample(weights: Seq[Double]): Int = {
      val probs = weights.scanLeft(0.0)(_ + _)
      val i     = probs.search(random.nextDouble() * probs.last).insertionPoint - 1
      i
    }
  }
}
