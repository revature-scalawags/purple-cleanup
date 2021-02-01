package relatedHashtags

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import util.FileWriter.writeDataFrameToFile
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object RelatedHashtags {

  /**
    * Find the top 10 Hashtags used with the COVID hashtag
    *
    * @param spark
    */
  def getHashtagsWithCovid(spark: SparkSession): Unit = {
    //What are the top 10 commonly used hashtags used alongside COVID hashtags?
    val staticDf = spark.read.json("s3a://adam-king-848/data/twitter_data.json")
    questionHelper(spark, staticDf)
  }

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
