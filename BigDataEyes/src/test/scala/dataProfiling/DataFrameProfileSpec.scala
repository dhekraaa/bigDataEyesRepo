package dataProfiling

import dataDictionary.PartitioningDictionary.WriteDiskPartitionsReport
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import persistance.KafkaProducer._
import persistance.DiskWriter._

class DataFrameProfileSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("DataCatalog")
    .getOrCreate()

  import spark.implicits._

  "" should "" in {
    val profiledResult = DataFrameProfile(baseballDf).toDataFrame
    profiledResult.show()
    writeToKafka("filesDatasProfiler",profiledResult,"columnName")
   // writeToDisk(profiledResult,"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dataFrameProfilingReports")


  }
  "scenario2" should "" in{
    val profiledResult = DataFrameProfile(data).toDataFrame
    profiledResult.show()
    writeToKafka("filesDatasProfiler",profiledResult,"columnName")
    //writeToDisk(profiledResult,"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dataFrameProfilingReports")
  }
  val data : DataFrame = spark.read.option("header",true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv")

  def baseballDf: DataFrame = {
    Seq(
      ("Mets", "1986", "New York", "nick"),
      ("Yankees", "2009", "New York", "dave"),
      ("Cubs", "2016", "Chicago", "bruce"),
      ("White Sox", "2005", "Chicago", null),
      ("Nationals", "", "Washington", null)
    ).toDF("team", "lastChampionship", "city", "fan")
  }
}
