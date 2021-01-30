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
    * @param df
    * @param region Specific region
    */
  def getHashtagsByRegion(spark: SparkSession, df: DataFrame, region: String): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    val newdf = generateDF(spark, df)

    // If we are passed a region, we need to filter by it
    // Otherwise we present all of the information
    if (region != null) {
      val sorteddf = newdf
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
      val sorteddf = newdf
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
    * @param df
    */
  def getHashtagsByRegionAll(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    var outputFilename: String = null

    val newdf = generateDF(spark, df)
      //explode List(Hashtags),Region to own rows resulting in Row(Hashtag,Region)
      .select(functions.explode($"Hashtags").as("Hashtag"), $"Region")
      //group by the same Region and Hashtag
      .groupBy("Region", "Hashtag")
      //count total of each Region/Hashtag appearance
      .count()

    val greatdf = newdf
      .orderBy(functions.desc("Count"))

    outputFilename = s"hbr-all-$startTime"
    writeDataFrameToFile(greatdf, outputFilename)

    RegionDictionary.getRegionList.foreach(region => {
      val bestdf = newdf
        .filter($"Region" === region)
        .orderBy(functions.desc("Count"))

      outputFilename = s"hbr-${region.replaceAll("\\s+","")}-$startTime"
      writeDataFrameToFile(bestdf, outputFilename)
    })
  }


  /** 
    *
    * @param spark
    * @param df
    * @return
    */
  private def generateDF(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val newDF = df
      .filter(!functions.isnull($"place"))
      .select($"entities.hashtags.text", $"place.country")
      //map to Row(List(Hashtags),Region)
      .map(tweet => {
        (tweet.getList[String](0).toList.map(_.toLowerCase()), RegionDictionary.reverseMapSearch(tweet.getString(1)))
      })
      .withColumnRenamed("_1", "Hashtags")
      .withColumnRenamed("_2", "Region")

      newDF
  }
}
