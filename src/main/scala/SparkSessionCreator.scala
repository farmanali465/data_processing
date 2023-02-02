import PipelineJob.logger
import org.apache.spark.sql.SparkSession

object SparkSessionCreator {

  def sparkSessionCreate(): SparkSession = {
    try {
      logger.info("Creating Spark Session")
      SparkSession
        .builder()
        .master("local[*]")
        .appName("interview_de")
        .getOrCreate()
    }
  }
}
