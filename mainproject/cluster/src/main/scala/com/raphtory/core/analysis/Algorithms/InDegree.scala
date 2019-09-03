package com.raphtory.core.analysis.Algorithms

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class InDegree extends Analyser {

  //To obtain the in degree distribution of the network we need to know the number of time in where an specific degree took place. For example,
  // 10  nodes had an in-degree of 2 in a given time .
  // For this we need to loop over the vertices in the system and get their ingoing neighbours.
  //Then we store that to a results array that stores all the in-degrees. For the given example in the array we will find a value similar to :
  // (2,2,2,2,2,2,2,2,2,2) which is then grouped by its identifier, in this case the number 2. The output value is 2,10 and passed to the Live Analysis.

  //Initialisation of the file in where the output will be written is done.
  //the resulys sent by the GabMiningDistribAnalyserIn, as received and parsed to get the tuples sent.
  //in where something like (2,10) will be received, meaning the degree 2 had 10 occurences.

  //then a reducer is performed to get all the other tuples containing the occurences of the in-degree 2. Then the final
  // list is written to the file.

  override def analyse()(implicit workerID: WorkerID): Any = {
    var results = ArrayBuffer[Int]()
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      val totalEdges = vertex.getIngoingNeighbors.size
      results += totalEdges
    })
    // println("THIS IS HOW RESULTS LOOK: "+ results.groupBy(identity).mapValues(_.size))
    results.groupBy(identity).mapValues(_.size).toList
  }

  override def setup()(implicit workerID: WorkerID): Any = {

  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any]): Unit = {
    val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    var finalResults = ArrayBuffer[(Int, Int)]()

    for (kv <- results) {
      //println("KV RESULTS: "+ kv )
      for (pair <- kv.asInstanceOf[List[(Int, Int)]]) {
        finalResults += pair
      }

    }

    val currentDate = LocalDateTime.now()
    //val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    var degrees = finalResults.groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _)).toList.sortBy(_._1) //.foreach(println)
    for ((degree, total) <- degrees) {
      var text = currentDate + "," + degree + "," + total
      Utils.writeLines("results/distribLAM.csv", text)

    }

  }


override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long): Unit = {
      val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
      Utils.writeLines(output_file,"Date,InDegree,Total")

      //Wed Aug 10 04:59:06 BST 2016
      val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
      val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

      var finalResults = ArrayBuffer[(Int, Int)]()

      for (kv <- results) {
        // println("KV RESULTS: " + kv)
        for (pair <- kv.asInstanceOf[List[(Int, Int)]]) {
          finalResults += pair
        }

      }

      val currentDate = new Date(timestamp)
      val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
      var degrees = finalResults.groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _)).toList.sortBy(_._1) //.foreach(println)
      for ((degree, total) <- degrees) {
        var text = formattedDate + "," + degree + "," + total
        Utils.writeLines(output_file, text)

      }
    }

  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSize: Long): Unit = {}
}

