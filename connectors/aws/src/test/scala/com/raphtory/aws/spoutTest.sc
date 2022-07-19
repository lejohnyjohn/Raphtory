import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.raphtory.Raphtory
import com.raphtory.aws.AwsSpoutTest.{awsS3SpoutBucketName, s3Client}
import com.raphtory.aws.{AwsCredentials, AwsS3Connector}

val config                        = Raphtory.getDefaultConfig()
val awsS3SpoutBucketName          = config.getString("raphtory.spout.aws.local.spoutBucketName")
val awsS3SpoutBucketPath          = config.getString("raphtory.spout.aws.local.spoutBucketPath")
val credentials: AwsCredentials = AwsS3Connector().getAWSCredentials()

val s3Client: AmazonS3 = AmazonS3ClientBuilder
  .standard()
  .withCredentials(
    new AWSStaticCredentialsProvider(credentials)
  )
  .build()

val s3object = s3Client.listObjects(new ListObjectsRequest().withBucketName(awsS3SpoutBucketName).withDelimiter("/").withPrefix("CompaniesHouse/"))
