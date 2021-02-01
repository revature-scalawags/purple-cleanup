package main

import org.apache.spark.sql.SparkSession

import relatedHashtags.RelatedHashtags
import util.FileWriter


object Main {
  def main(args: Array[String]) = {
    
    // Original jsonPath = s3a://adam-king-848/data/twitter_data.json
    // jsonPath currently points to test data
    val jsonPath = "twitter_data.json"

    val spark = SparkSession
    .builder()
    .appName("Related-Hashtags")
    .master("local[4]")
    .getOrCreate()

    // twitterDF is the base DataFrame created from the contents of an input json file.
    val twitterDF = FileWriter.getDataFrameFromJson(spark, jsonPath)

    RelatedHashtags.getHashtagsWithCovid(spark, twitterDF)
    }
  }