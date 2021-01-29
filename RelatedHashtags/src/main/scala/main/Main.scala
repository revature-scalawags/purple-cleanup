package main
import relatedHashtags.RelatedHashtags._
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("relatedHashtags")
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    getHashtagsWithCovid(spark)
  }
}