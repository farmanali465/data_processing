import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class UtilitiesTestSpec extends FunSuite with BeforeAndAfterEach {

  val amenitiesSchema = new StructType()
    .add("property_id", IntegerType, true)
    .add("amenity_id", StringType, true)
  val propertiesSchema = new StructType()
    .add("property_id", IntegerType, true)
    .add("active", BooleanType, true)
    .add("discovered_dt", DateType, true)
  private val master = "local"
  private val appName = "pipelineTest"
  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  test("Testing Spark Session ") {
    val sparkSession = SparkSessionCreator.sparkSessionCreate()
    assert(!(sparkSession == None))
  }

  test("Test Csv Read function") {
    val sparkSession = spark

    val dataFrame = DataExtractor.rawCsvReader(spark, amenitiesSchema, "amenities_test.txt")
    assert(dataFrame.count() == 3)
  }
  test("Test Json Read Function") {
    val sparkSession = spark
    val dataFrame = DataExtractor.rawJsonReader(spark, propertiesSchema, "properties_test.json")
    assert(dataFrame.count() == 3)
  }
  test("Test Data Formatter Function") {
    val sparkSession = spark
    var dataFrame = DataExtractor.rawCsvReader(spark, amenitiesSchema, "amenities_test.txt")
    dataFrame = DataTransformer.formatData(dataFrame)
    assert(dataFrame.count() == 18)
  }
  test("Test Active Properties By Amenities Function") {
    val sparkSession = spark
    var dataFrame = DataExtractor.rawCsvReader(spark, amenitiesSchema, "amenities_test.txt")
    dataFrame = DataTransformer.formatData(dataFrame)
    var dataFrame1 = DataExtractor.rawJsonReader(spark, propertiesSchema, "properties_test.json")
    dataFrame = DataTransformer.activePropertiesByAmenities(dataFrame1, dataFrame)
    assert(dataFrame.count() == 18)
  }
  test("Test summarized Count By Discovery Dates Function") {
    val sparkSession = spark
    var dataFrame = DataExtractor.rawJsonReader(spark, propertiesSchema, "properties_test.json")
    dataFrame = DataTransformer.summarizedCountByDiscoveryDates(dataFrame)
    assert(dataFrame.count() == 2)
  }

  override def afterEach(): Unit = {
    spark.stop()
  }
}

case class Person(name: String, age: Int)