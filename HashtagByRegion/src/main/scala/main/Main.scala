package main

import org.apache.spark.sql.SparkSession

import hashtagByRegion.HashtagByRegion
import util.FileUtil


/**
  * QUESTION 7: What are the hashtags used to describe COVID-19 by Region (e.g. #covid, #COVID-19, #Coronavirus, #NovelCoronavirus)?
  */
object Main {
  def main(args: Array[String]) = {

    // jsonPath currently points to test data
    ////// After an EMR cluster has been established, update path to "s3a://covid-analysis-p3/datalake/twitter-covid/01-31-21-twitter_data.json" //////////
    val jsonPath = "s3a://covid-analysis-p3/datalake/twitter-covid/test_twitter_data.json"

    val spark = SparkSession
      .builder()
      .appName("Hashtag-By-Region")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // twitterDF is the base DataFrame created from the contents of an input json file.
    val twitterDF = FileUtil.getDataFrameFromJson(spark, jsonPath)

    // If no arguments are passed in, run getHashtagsByRegionAll
    // otherwise, run getHashtagsByRegion using args(0) as the region parameter.
    if (args.isEmpty) {
      HashtagByRegion.getHashtagsByRegionAll(spark, twitterDF)
    } else {
      val region = args(0)
      HashtagByRegion.getHashtagsByRegion(spark, twitterDF, region)
    }

    spark.stop()
  }
}