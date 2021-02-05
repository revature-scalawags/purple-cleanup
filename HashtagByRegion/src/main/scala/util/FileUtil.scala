package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


/** FileUtil contains methods for reading and writing files. */
object FileUtil {


  /** Writes a DataFrame to a CSV file and pushes it to an S3 Bucket
    *
    * @param dataFrame DataFrame to be written to file.
    * @param outputFilename Name of the output file.
    * @param maxRecords The number of records to be saved to the output file.
    */
  def writeDataFrameToFile(dataFrame: DataFrame, outputFilename: String, maxRecords: Int = 100) = {
    dataFrame
      .limit(maxRecords)
      .write
      .csv(s"s3a://covid-analysis-p3/datawarehouse/twitter-covid/HashtagByRegion/${outputFilename}")
  }


  /** Reads in a JSON file from the supplied path and returns its data
    * as a DataFrame
    *
    * @param spark Current SparkSession
    * @param path Path to the input JSON file
    * @return The DataFrame built from input JSON file
    */
  def getDataFrameFromJson(spark: SparkSession, path: String): DataFrame = {
    spark.read.json(path)
  }
}