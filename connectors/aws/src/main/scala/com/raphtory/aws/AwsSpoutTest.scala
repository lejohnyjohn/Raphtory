package com.raphtory.aws

import com.raphtory.Raphtory
import com.raphtory.algorithms.filters.{EdgeFilter, VertexFilter, VertexFilterGraphState, VertexQuantileFilter}
import com.raphtory.algorithms.generic.{EdgeList, NodeList}
import com.raphtory.algorithms.generic.centrality.{Degree, PageRank}
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.aws.graphbuilders.officers.OfficerToCompanyGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import scala.util.Using

/**
 * Tests the AWS S3 Spout and Sink, requires bucket name and bucket path that you would like to ingest.
 * Also requires bucket to output results into. Both set in application.conf.
 */

object AwsSpoutTest {

//  val config = Raphtory.getDefaultConfig()
//  val awsS3SpoutBucketName = config.getString("raphtory.spout.aws.local.spoutBucketName")
//  val awsS3SpoutBucketKey = config.getString("raphtory.spout.aws.local.spoutBucketPath")
  //    val awsS3OutputFormatBucketName = config.getString("raphtory.spout.aws.local.outputBucketName")

//      val source               = AwsS3Spout("pometry-data","CompaniesHouse")
def main(args: Array[String]) {

  val source = FileSpout("/Users/rachelchan/Documents/DodgyDirectors/", regexPattern = "^.*\\.([jJ][sS][oO][nN]??)$", recurse = true)
//  val source = FileSpout("/home/ubuntu/DodgyDirectors", regexPattern = "^.*\\.([jJ][sS][oO][nN]??)$", recurse = true)
  val builder = new OfficerToCompanyGraphBuilder()
  val output = FileSink("/tmp/dodgydirectorswindow")
  val graph = Raphtory.load[String](source, builder)
//    graph
//      .at(368301600, )
//      .past()
//      .execute(EdgeList())
//      .writeTo(output)
//      .waitForJob()

  graph
    .range("2000-01-01", "2022-07-25", "1 day")
    .window("2 days", Alignment.END)
    .execute( VertexQuantileFilter[Long](
      lower = 0,
      upper = 100
    ) -> EdgeList())
    .writeTo(output)
  //    .execute(EdgeList())
//    .writeTo(output)
//          VertexFilter(vertex => {
//          vertex. == "Officer ID" && vertex.outDegree > 50000 || vertex.Type() == "Company Number"
//        })
  //          ->

  }
  //    Raphtory.streamIO(spout = source, graphBuilder = builder).use { graph =>
  //      IO {
  //        graph
  //          .at(32674)
  //          .past()
  //          .execute(EdgeList())
  //          .writeTo(output)
  //          .waitForJob()
  //        ExitCode.Success
  //      }
  //    }


}
