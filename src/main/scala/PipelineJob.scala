import org.apache.log4j.Logger
import org.apache.spark.sql.types._

object PipelineJob {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // create spark session
    try {
      logger.info("Starting ETL Pipeline")
      logger.info("Creating Spark Session")
      val spark = SparkSessionCreator.sparkSessionCreate()
      val amenitiesSchema = new StructType()
        .add("property_id", IntegerType, true)
        .add("amenity_id", StringType, true)
      val propertiesSchema = new StructType()
        .add("property_id", IntegerType, true)
        .add("active", BooleanType, true)
        .add("discovered_dt", DateType, true)
      logger.info("Loading Data from local")
      var amenitiesDataframe = DataExtractor.rawCsvReader(spark, amenitiesSchema, "amenities.txt")
      var propertiesDataframe = DataExtractor.rawJsonReader(spark, propertiesSchema, "properties.json")
      amenitiesDataframe = DataTransformer.formatData(amenitiesDataframe)
      logger.info("Writing Data to local")
      DataLoader.parquetSaver(propertiesDataframe, "/output/PropertiesData/")
      DataLoader.parquetSaver(amenitiesDataframe, "/output/AmenitiesData/")
      logger.info("Loading Parquet Data from local")
      amenitiesDataframe = DataExtractor.rawParquetReader(spark, "/output/AmenitiesData/")
      propertiesDataframe = DataExtractor.rawParquetReader(spark, "/output/PropertiesData/")
      logger.info("Applying transformation on data ")
      val activePropertiesDataframe = DataTransformer.activePropertiesByAmenities(propertiesDataframe, amenitiesDataframe)
      DataLoader.jsonSaver(activePropertiesDataframe, "/output/ActivePropertiesDataframe/")
      val summarizedCountDataframes = DataTransformer.summarizedCountByDiscoveryDates(propertiesDataframe)
      DataLoader.csvSaver(summarizedCountDataframes, "/output/SummarizedCountDataframes/")
      spark.stop()
      logger.info("Pipeline Runs Successfully")

    }
    catch {
      case e: Exception => logger.error("Exception while running ETL pipeline")
    }

  }
}
