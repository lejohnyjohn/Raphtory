package com.raphtory.aws

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.sinks.FileSink
import com.typesafe.config.ConfigFactory

/**
  * Test to use the AWS S3 Spout, requires bucket name and bucket path that you would like to ingest,
  * set in application.conf.
  */

object AwsSpoutTest extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    val config               = ConfigFactory.load()
    val awsS3SpoutBucketName = config.getString("raphtory.spout.aws.local.spoutBucketName")
    val awsS3SpoutBucketPath = config.getString("raphtory.spout.aws.local.spoutBucketPath")
    val source               = AwsS3Spout(awsS3SpoutBucketName, awsS3SpoutBucketPath)
    val builder              = new LotrGraphBuilder()
    Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
      IO {

        graph
          .at(32674)
          .past()
          .execute(EdgeList())
          .writeTo(FileSink("/tmp/raphtory"))
          .waitForJob()
        ExitCode.Success
      }
    }
  }
}
