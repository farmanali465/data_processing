
import PipelineJob.logger
import org.apache.spark.sql.{DataFrame, SaveMode}
// save your output from the various objects here

object DataLoader {

  // function to save a fitted pipeline
  def parquetSaver(dataFrame: DataFrame, path: String): Unit = {
    try {
      dataFrame
        .write
        .mode(saveMode = SaveMode.Overwrite)
        .parquet(path)
    }
    catch {
      case e: Exception => logger.error("Unable to write Parquet on given path")
    }


  }

  def jsonSaver(dataFrame: DataFrame, path: String): Unit = {
    try {
      dataFrame
        .write
        .mode(saveMode = SaveMode.Overwrite)
        .json(path)
    }
    catch {
      case e: Exception => logger.error("Unable to write JSON file on given path")
    }

  }

  // function to save predictions
  def csvSaver(dataFrame: DataFrame, path: String): Unit = {
    try {
      dataFrame
        .write
        .mode(saveMode = SaveMode.Overwrite)
        .csv(path = path)
    }
    catch {
      case e: Exception => logger.error("Unable to write JSON file on given path")
    }

  }
}