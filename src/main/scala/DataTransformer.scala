import PipelineJob.logger
import org.apache.spark.sql.functions.{col, explode, month, year}
import org.apache.spark.sql.{DataFrame, functions}

// clean raw data frames here - format columns and transform

object DataTransformer {

  // def function to format data correctly
  def formatData(dataFrame: DataFrame): DataFrame = {
    try {
      logger.info("Formatting amenities data by exploding and splitting")
      dataFrame.withColumn("explode_amenity_id", explode(functions.split(col("amenity_id"), "\\|")))
        .drop(col("amenity_id"))
        .withColumnRenamed("explode_amenity_id", "amenity_id")
    }
    catch {
      case e: Exception => logger.error("Unable to format a amenities data")
        dataFrame
    }

  }

  // def function to find active properties by amenities data correctly
  def activePropertiesByAmenities(propertiesDataframe: DataFrame, amenitiesDataframe: DataFrame): DataFrame = {
    try {
      logger.info("Finding Active Properties By Amenities")
      propertiesDataframe.join(amenitiesDataframe, propertiesDataframe.col("property_id") === amenitiesDataframe.col("property_id"), "inner")
        .filter(propertiesDataframe.col("active") === "true")
        .select(propertiesDataframe.col("property_id"), amenitiesDataframe.col("amenity_id"))
    }

    catch {
      case e: Exception => logger.error("Unable to find Active Properties By Amenities")
        propertiesDataframe
    }
  }

  // function to calculate summarized Count By Discovery Dates
  def summarizedCountByDiscoveryDates(dataFrame: DataFrame): DataFrame = {
    try {
      logger.info("Calculating summarized Count By Discovery Dates")
      dataFrame.withColumn("month", month(col("discovered_dt"))).withColumn("year", year(col("discovered_dt")))
        .groupBy("year", "month").count().orderBy("year", "month")
    }
    catch {
      case e: Exception => logger.error("Unable to calculate summarized Count By Discovery Dates")
        dataFrame
    }
  }

}
