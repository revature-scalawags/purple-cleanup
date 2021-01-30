package main

import org.apache.spark.sql.SparkSession

import hashtagByRegion.HashtagByRegion
import util.FileUtil

object Main {
  def main(args: Array[String]) = {

    // Original jsonPath = s3a://adam-king-848/data/twitter_data.json
    // jsonPath currently points to test data
    val jsonPath = "twitter_data.json"

    val spark = SparkSession
      .builder()
      .appName("Hashtag-By-Region")
      .master("local[4]")
      .getOrCreate()

    val twitterDF = FileUtil.getDataFrameFromJson(spark, jsonPath)

    if (args.isEmpty) {
      HashtagByRegion.getHashtagsByRegionAll(spark, twitterDF)
    } else {
      val region = args(0)
      HashtagByRegion.getHashtagsByRegion(spark, twitterDF, region)
    }
  }
}