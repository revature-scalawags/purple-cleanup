package util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder


/**
  * Singleton object that provides static methods for working with AWS S3
  */
object S3Client{


  /**
    * Builds the AmazonS3 object with credentials for accessing data stored on the cloud
    *
    * @param key S3 Access token
    * @param secret S3 Secret token
    * @return The client AmazonS3 object
    */
  def buildS3Client(key: String, secret: String): AmazonS3 = {
    val credentials = new BasicAWSCredentials(key, secret)
    val client = AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withRegion("us-east-1")
      .build();
    client
  }
}