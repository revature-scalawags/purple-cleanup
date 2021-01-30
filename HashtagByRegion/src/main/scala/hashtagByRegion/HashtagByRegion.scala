package hashtagByRegion

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import util.FileUtil.writeDataFrameToFile


/** HashtagByRegion is a singleton object that contains statically accessible methods for working with
  * Twitter data based on COVID-19 related hashtags.
  * 
  */
object HashtagByRegion {


  /**
    *
    * @param spark Current SparkSession
    * @param df Base DataFrame
    * @param region Specific region
    */
  def getHashtagsByRegion(spark: SparkSession, df: DataFrame, region: String): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    // hashtagDF is a DataFrame that contains 
    val hashtagDF = generateDF(spark, df)

    // If we are passed a region, we need to filter by it
    // Otherwise we present all of the information
    if (region != null) {
      val sorteddf = hashtagDF
        .filter($"region" === region)
        //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
        .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
        //group by the same Region and Hashtag
        .groupBy("Region", "Hashtag")
        //count total of each Region/Hashtag appearance
        .count()
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-${region.replaceAll("\\s+","")}-$startTime"
      writeDataFrameToFile(sorteddf, outputFilename)
    } else {
      val sorteddf = hashtagDF
        //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
        .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
        //group by the same Region and Hashtag
        .groupBy("Region", "Hashtag")
        //count total of each Region/Hashtag appearance
        .count()
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-AllRegions-$startTime"
      writeDataFrameToFile(sorteddf, outputFilename)
    }
  }


  /** 
    *
    * @param spark Current SparkSession
    * @param df Base DataFrame
    */
  def getHashtagsByRegionAll(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    // startTime is only used as a part of the output file name
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    val hashtagDF = generateDF(spark, df)
      // Explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
      .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
      // Group by the same Region and Hashtag
      .groupBy("Region", "Hashtag")
      // Count total of each Region/Hashtag appearance
      .count()

    val greatdf = hashtagDF
      .orderBy(functions.desc("Count"))

    outputFilename = s"hbr-all-$startTime"
    writeDataFrameToFile(greatdf, outputFilename)

    RegionDictionary.getRegionList.foreach(region => {
      val bestdf = hashtagDF
        .filter($"Region" === region)
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-${region.replaceAll("\\s+","")}-$startTime"
      writeDataFrameToFile(bestdf, outputFilename)
    })
  }


  /** Takes an input base DataFrame, filters out the hastag data, and returns that
    * data as a new DataFrame 
    *
    * @param spark Current SparkSession
    * @param df Base DataFrame
    * @return New DataFrame that contains only the hashtag data
    */
  private def generateDF(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val hashtagDF = df
      // Filter out tweets that do not contain location data.
      .filter(!functions.isnull($"place"))
      // Select hashtag text and country Columns
      .select($"entities.hashtags.text", $"place.country")
      // Map to Row(List(Hashtags),Region)
      .map(tweet => {
        (tweet.getList[String](0).toList.map(_.toLowerCase()), RegionDictionary.reverseMapSearch(tweet.getString(1)))
      })
      // Rename the columns
      .withColumnRenamed("_1", "Hashtags")
      .withColumnRenamed("_2", "Region")

      hashtagDF
  }
}
