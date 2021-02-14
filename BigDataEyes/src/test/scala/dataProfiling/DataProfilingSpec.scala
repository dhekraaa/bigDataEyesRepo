package dataProfiling

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dataProfiling.DataProfiling._
import persistance.DiskWriter.writeToDisk
import persistance.KafkaProducer._

class DataProfilingSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("DataCatalog")
    .getOrCreate()

  val expectedResultForReadData: DataFrame = spark.read.option("description", value = true).option("header", value = true).csv("src/test/resources/sales.csv")
  val expectedResultForReadDataScenario2: DataFrame = spark.read.option("description", value = true).option("header", value = true).csv("src/test/resources/sales.csv")

  val expectedResultForDataProfiler: DataFrame = expectedResultForReadData.describe()

  "readData" should "read all data from a given path" in {
    Given("data path")
    val dataPath = "src/test/resources/sales.csv"
    When("readData is invoked")
    val dataRead = readData(dataPath)
    Then("all data from a given file should be returned ")
    val expectedResultDF = expectedResultForReadData
    dataRead.collect() should contain theSameElementsAs expectedResultDF.collect()
  }

  "dataProfiler" should "profile the given data " in {
    Given("data")
    val data = expectedResultForReadData
    When("dataProfiler is invoked")
    val profile = dataProfiler(data)
    Then("the profile of the given data should be returned")
    val expectedResultDF = expectedResultForDataProfiler
    profile.collect() should contain theSameElementsAs expectedResultDF.collect()
    profile.show()
  }
  "DataProfiledFormer" should "" in {
    val data: DataFrame = spark.read.option("header",true).csv("src/test/resources/sales.csv")
    val d2 = DataProfiledFormer(data)
    //writeToDisk(d2,"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\qualityProfilingReports")

    writeToKafka("fileDataProfilerTopic",d2,"ColumnName")
  }

}
