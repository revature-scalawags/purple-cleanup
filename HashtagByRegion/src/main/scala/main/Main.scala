package main

import org.apache.spark.sql.SparkSession

import hashtagByRegion.HashtagByRegion
import util.FileUtil


/**
  * QUESTION 7: What are the hashtags used to describe COVID-19 by Region (e.g. #covid, #COVID-19, #Coronavirus, #NovelCoronavirus)?
  */
object Main {
  def main(args: Array[String]) = {

    // Original jsonPath = s3a://adam-king-848/data/twitter_data.json
    // jsonPath currently points to test data
    val jsonPath = "s3://covid-analysis-p3/datalake/01-31-21-twitter_data.json"

    val spark = SparkSession
      .builder()
      .appName("Hashtag-By-Region")
      .master("local[4]")
      .getOrCreate()

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
  }
}