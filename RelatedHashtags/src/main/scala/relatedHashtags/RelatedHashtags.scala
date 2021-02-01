package relatedHashtags

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import util.FileWriter.writeDataFrameToFile
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * RelatedHashtags is a singleton object that contains statically accessible methods for working with
  * Twitter data based on COVID-19 related hashtags.
  */
object RelatedHashtags {

  /**
    * Takes in a base DataFrame as a parameter, filters the DataFrame by hashtags and saves
    * the resulting DataFrame as a csv file.
    *
    * @param spark Current SparkSession
    * @param df Base DataFrame
    */
  def getHashtagsWithCovid(spark: SparkSession, df: DataFrame): Unit = {
    //What are the top 10 commonly used hashtags used alongside COVID hashtags?
    questionHelper(spark, df)
  }

  /**
    * Private helper method for getHashtagsWithCovid.
    *
    * @param spark Current SparkSession
    * @param df Base DataFrame
    */
  private def questionHelper(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val covidRelatedWordsList = CovidTermsList.getTermsList
    val newDf = df
      .select($"entities.hashtags.text")
      //map to Row(List(Hashtags))
      .map(tweet => {
        tweet.getList[String](0).toList.map(_.toLowerCase())
      })
      .withColumnRenamed("value", "Hashtags")
      //filter to only lists with greater than 1 hashtags (2+)
      .filter(functions.size($"Hashtags").gt(1))
      //filter to only lists that contain a word from our filter list
      .filter(hashtags => {
        val hashtagList = hashtags.getList[String](0)
        //this filter statement seems inefficient
        hashtagList.exists(
          hashtag => covidRelatedWordsList.exists(
            covidHashtag => hashtag.contains(covidHashtag.toLowerCase())
          )
        )
      })
      //explode out all remaining List(Hashtags)
      .select(functions.explode($"Hashtags").as("Hashtag"))
      //remove all items that are on the our filter list
      .filter(hashtag => {
        val hashtagStr = hashtag.getString(0)
        !covidRelatedWordsList.exists(
          covidHashtag => hashtagStr.toLowerCase().contains(covidHashtag.toLowerCase())
        )
      })
      .groupBy($"Hashtag")
      .count()
      .orderBy(functions.desc("Count"))

    val outputFilename: String = s"hwc-full-$startTime"
    writeDataFrameToFile(newDf, outputFilename)
  }
}
