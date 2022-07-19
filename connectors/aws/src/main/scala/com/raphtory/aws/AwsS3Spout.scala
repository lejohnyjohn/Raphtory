package com.raphtory.aws

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsRequest, ObjectListing, S3Object, S3ObjectSummary}
import com.raphtory.api.input.Spout
import java.io.{BufferedReader, InputStream, InputStreamReader}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.jdk.javaapi.CollectionConverters.asScala

/**
  * @param awsS3SpoutBucketName
  * @param awsS3SpoutBucketPath
  *
  * The AwsS3Spout takes in the name and path of the AWS S3 bucket that you would like
  * to ingest into Raphtory, usually this is set in your tests.
  *
  * It builds an S3 client using credentials obtained through providing access keys.
  * The data is streamed from AWS S3 until null is reached.
  */

class AwsS3Spout(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String, recurse: Boolean = false) extends Spout[String] {

  var list: List[String] = _
  var s3object: S3Object = _
  var s3Reader: BufferedReader = _
  var transformedList = new ListBuffer[String]()
  val credentials: AwsCredentials = AwsS3Connector().getAWSCredentials()

  private val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withCredentials(
      new AWSStaticCredentialsProvider(credentials)
    )
    .build()


  def map[T](s3: AmazonS3, bucket: String, prefix: String)(f: (S3ObjectSummary) => T) = {
    def scan(acc: List[T], listing: ObjectListing): List[T] = {
      val summaries = asScala[S3ObjectSummary](listing.getObjectSummaries)
      val mapped = (for (summary <- summaries) yield f(summary)).toList

      if (!listing.isTruncated) acc ++ mapped
      else scan(acc ::: mapped, s3.listNextBatchOfObjects(listing))
    }
    scan(List(), s3.listObjects(bucket, prefix))
  }


//      if (recurse) {
//          list.map { key =>
//            transformedList += key.split("/")(1)
//          }
//      } else {
//      list.foreach { key =>
//        s3object = s3Client.getObject(new GetObjectRequest(awsS3SpoutBucketName, key))
//        s3Reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent))
//      }
//    }



  val listOfKeys =  map(s3Client, awsS3SpoutBucketName, awsS3SpoutBucketPath)(s =>  {
       s3Reader = new BufferedReader(new InputStreamReader(s3Client.getObject(new GetObjectRequest(awsS3SpoutBucketName, s.getKey)).getObjectContent))
   })

  override def hasNext: Boolean = {
    s3Reader != null
  }

  override def next(): String = {
    s3Reader.readLine()
  }

  override def close(): Unit =
    logger.debug(s"Spout for AWS '$awsS3SpoutBucketName' finished")

  override def spoutReschedules(): Boolean = false
}

object AwsS3Spout {

  def apply(awsS3SpoutBucketName: String, awsS3SpoutBucketPath: String, recurse: Boolean = false) =
    new AwsS3Spout(awsS3SpoutBucketName, awsS3SpoutBucketPath, recurse)
}
