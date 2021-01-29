package util

import org.apache.spark.sql.DataFrame


/** FileWriter contains a single method for writing a DataFrame to a CSV file and saving it to an S3 bucket. */
object FileWriter {

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
        .format("csv")
        .save(s"s3a://adam-king-848/results/purple/$outputFilename")  // Depending on our S3 structure, this may need to be changed.
  }
}