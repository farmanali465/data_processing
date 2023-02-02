import PipelineJob.logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileNotFoundException

object DataExtractor {

  def rawCsvReader(sparkSession: SparkSession, amenitiesSchema: StructType, path: String): DataFrame = {
    // load csv from Local

    try {
      logger.info("Reading CSV File")
      sparkSession.read.format("csv")
        .option("delimiter", " ")
        .schema(amenitiesSchema)
        .load(path)
    }
    catch {
      case e: FileNotFoundException => logger.error("File not found on given path")
        sparkSession.emptyDataFrame
    }
  }

  def rawJsonReader(sparkSession: SparkSession, propertiesSchema: StructType, path: String): DataFrame = {

    // load json from local
    try {
      logger.info("Reading JSON File")
      sparkSession.read.format("json")
        .schema(propertiesSchema)
        .load(path)
    }
    catch {
      case e: FileNotFoundException => logger.error("File not found on given path")
        sparkSession.emptyDataFrame
    }

  }

  def rawParquetReader(sparkSession: SparkSession, path: String): DataFrame = {

    // load parquet from local
    try {
      logger.info("Reading PARQUET File")
      sparkSession.read.format("parquet")
        .load(path)
    }
    catch {
      case e: FileNotFoundException => logger.error("File not found on given path")
        sparkSession.emptyDataFrame
    }

  }
}