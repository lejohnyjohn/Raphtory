package com.raphtory.dev.lotr

import com.raphtory.core.actors.spout.Spout
import scala.collection.mutable

class LOTRSpout extends Spout[String] {
 val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
    val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //
    val filename = "/home/tsunade/qmul/raphtory/src/main/scala/com/raphtory/dev/lotr/lotr.csv"//directory + "/" + fileName
  val fileQueue = mutable.Queue[String]()
	
  override def setupDataSource(): Unit = {
    fileQueue++=
      scala.io.Source.fromFile(filename)
        .getLines
  }//no setup

  override def generateData(): Option[String] = {
    if(fileQueue isEmpty){
      dataSourceComplete()
      None
    }
    else
      Some(fileQueue.dequeue())
  }
  override def closeDataSource(): Unit = {}//no file closure already done
}
