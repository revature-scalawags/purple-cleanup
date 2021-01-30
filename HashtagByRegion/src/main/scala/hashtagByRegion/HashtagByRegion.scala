package hashtagByRegion

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

import util.FileUtil.writeDataFrameToFile


/** HashtagByRegion is a singleton object that contains statically accessible methods for working with
  * Twitter data based on COVID-19 related hashtags.
  * 
  */
object HashtagByRegion {


  /** Takes in a base DataFrame and a region as parameters, filters the DataFrame by hashtags and region,
    * and saves the resulting DataFrame as a csv file.
    *
    * @param spark Current SparkSession
    * @param df Base DataFrame
    * @param region Specific region
    */
  def getHashtagsByRegion(spark: SparkSession, df: DataFrame, region: String): Unit = {
    import spark.implicits._
    // startTime is only used as a part of the output file name
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    // hashtagDF is a DataFrame that contains 
    val hashtagDF = generateDF(spark, df)

    // Filter by region
    if (region != null) {
      val sortedDF = hashtagDF
        .filter($"region" === region)
        //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
        .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
        //group by the same Region and Hashtag
        .groupBy("Region", "Hashtag")
        //count total of each Region/Hashtag appearance
        .count()
        .orderBy(functions.desc("Count"))

      // Write data to file for specified region
      outputFilename = s"hbr-${region.replaceAll("\\s+","")}-$startTime"
      writeDataFrameToFile(sortedDF, outputFilename)
    }
  }


  /** Takes in a base DataFrame as a parameter, filters the DataFrame by hashtags,
    * and saves the resulting DataFrame as a csv file.
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

    // Creates a new DataFrame by sorting hashtagDF by "Count" in descending order
    val sortedDF = hashtagDF
      .orderBy(functions.desc("Count"))

    // Write data for all regions to single file
    outputFilename = s"hbr-all-$startTime"
    writeDataFrameToFile(sortedDF, outputFilename)

    // Write data for each region to individual files
    RegionDictionary.getRegionList.foreach(region => {
      val regionalDF = hashtagDF
        .filter($"Region" === region)
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-${region.replaceAll("\\s+","")}-$startTime"
      writeDataFrameToFile(regionalDF, outputFilename)
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
