package examples.lotr

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

object SixDegreesOfGandalf{
  def apply(seperation:Int): SixDegreesOfGandalf = new SixDegreesOfGandalf(Array(seperation.toString))
}

class SixDegreesOfGandalf(args: Array[String]) extends Analyser[List[(Int,Int)]](args){

  val SEP=args(0).toInt

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      val sep_state = vertex.messageQueue[Int].max -1
      if ((sep_state > 0) & (sep_state > vertex.getState[Int]("separation"))) {
        vertex.setState("separation", sep_state)
        vertex.messageAllNeighbours(sep_state)
      }
    }
  }

  override def setup(): Unit = {
    var sep_state = 0
    view.getVertices().foreach{vertex =>
      val name = vertex.getPropertyValue("name").getOrElse("")
      if (name == "Gandalf"){
        sep_state = SEP //user-defined parameter to determine degree of separation
        vertex.messageAllNeighbours(sep_state)
      }else{
        sep_state = 0
      }
      vertex.setState("separation", sep_state)

    }
  }

  override def returnResults(): List[(Int,Int)] =
    view.getVertices()
      .filter(vertex => vertex.getState[Int]("separation") > 0)
      .map(v => (v.ID(), v.getState[Int]("separation")))
      .groupBy(f => f._2)
      .map(f => (f._1, f._2.size)).toList

  override def defineMaxSteps(): Int = 100

  override def extractResults(results: List[List[(Int,Int)]]): Map[String,Any]  = {
    val endResults = results//.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Int, Int]]]
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum)
      val direct = if (grouped.size>0) grouped(SEP-1) else 0
      val total = grouped.values.sum
      val text = s"""{"total":${total},"direct":${direct}}"""
      println(text)
    } catch {
      case e: Exception => println("null")
    }
    Map[String,Any]()
  }}
