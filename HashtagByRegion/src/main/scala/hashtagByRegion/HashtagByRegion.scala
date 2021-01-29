package hashtagByRegion

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import util.FileWriter.writeDataFrameToFile
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


/** This singleton object contains statically accessible methods for working with
 *  Twitter data based on COVID-19 related hashtags.
 * 
 * QUESTION 1: What are the hashtags used to describe COVID-19 by Region (e.g. #covid, #COVID-19, #Coronavirus, #NovelCoronavirus)?
 */
object HashtagByRegion {


  /** 
    *
    * @param spark Current SparkSession
    * @param region Specific region
    */
  def getHashtagsByRegion(spark: SparkSession, region: String = null): Unit = {
    // This method only exists to read in the json file and then call another method.  Why?
    val staticDf = getJSON(spark, "s3a://adam-king-848/data/twitter_data.json")
    question1(spark, staticDf, region)
  }


  /**
    *
    * @param spark Current SparkSession
    */
  def getHashtagsByRegionAll(spark: SparkSession): Unit = {
    // This method only exists to read in the json file and then call another method.  Why?
    val staticDf = getJSON(spark, "s3a://adam-king-848/data/twitter_data.json")
    question1all(spark, staticDf)
  }


  /** Reads in a JSON file from the supplied path and returns its data
    * as a DataFrame
    *
    * @param spark Current SparkSession
    * @param path Path to the input JSON file
    * @return The DataFrame build from input JSON file
    */
  private def getJSON(spark: SparkSession, path: String): DataFrame = {
    spark.read.json(path)
  }


  /**
    *
    * @param spark
    * @param df
    * @param region
    */
  private def question1(spark: SparkSession, df: DataFrame, region: String): Unit = {
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



  private def question1all(spark: SparkSession, df: DataFrame): Unit = {
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
