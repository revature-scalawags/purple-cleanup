package main

import org.apache.spark.sql.SparkSession

import hashtagByRegion.HashtagByRegion

object Main {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Hashtag-By-Region")
      .master("local[4]")
      .getOrCreate()

    if (args.isEmpty) {
      HashtagByRegion.getHashtagsByRegionAll(spark)
    } else {
      val region = args(0)
      HashtagByRegion.getHashtagsByRegion(spark, region)
    }
  }
}