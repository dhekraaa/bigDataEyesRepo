package persistance

import java.util.{Calendar, Properties}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import persistance.KafkaProducer._
import dataDictionary.DataDictionaryWithCatalogApi._
import dataProfiling.DataProfiling.dataProfiler
import dataDictionary.ProcessingDataDictionary._
import dataProfiling.DataFrameProfile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel


class KafkaProducerSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[*]")
    .getOrCreate()

  val jdbcHostname = "localhost"
  val jdbcPort = 3306
  val jdbcDatabase = "banque"

  implicit val jdbcUrl: String = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

  val struct2: StructType =
    StructType(
      StructField("TableName", StringType, nullable = true) ::
        StructField("PartitionsTotalSize", LongType, nullable = false) ::
        StructField("PartitionNumber", LongType, nullable = false) ::
        StructField("PartitionsSize", StringType, nullable = false) ::
        StructField("PartitionsKey", StringType, nullable = false) ::
        StructField("MaxPartitionSize", LongType, nullable = true) ::
        StructField("MinPartitionSize", LongType, nullable = false) ::
        StructField("AvgPartitionSize", LongType, nullable = false) ::
        StructField("FilesNumberPerPartition", StringType, nullable = false) ::
        StructField("FilesSizePerPartition", StringType, nullable = false) ::
        StructField("PartitionRecommendation", StringType, nullable = true) ::
        StructField("KeyChoiceRecommendation", StringType, nullable = true) ::
        StructField("Status",StringType,nullable = true)::
        StructField("Date", StringType, nullable = false) ::
        Nil)


  val struct: StructType =
    StructType(
        StructField("transactionId", StringType, nullable = true) ::
        StructField("customerId", StringType, nullable = false) ::
        StructField("itemId", StringType, nullable = false) ::
        StructField("amountPaid", StringType, nullable = false) :: Nil)
  val TopicName: String = "datadictionary"
  val data: DataFrame = spark.read.schema(struct).csv("src/test/resources/sales.csv")


  "writeToKafka" should "write a given dataFrame to a given kafka topic " in {
    Given("topic name, data frame to send")
    val topicName: String = "datadictionary"
    val df = readData("src/test/resources/sales.csv")
    df.createTempView("sales")
    val resultOfGetDatabasesInfo = getAllDataBasesInformation()
    println(resultOfGetDatabasesInfo.count())
    val resultOfGetTablesInfo = getTablesInformation("default")
    val resultOfgetColumnsInformation = getColumnsInformation("sales")
    resultOfGetTablesInfo.show()
    resultOfGetDatabasesInfo.show()
    val resultOfProfiler = dataProfiler(data)
    val resultOfGetPartitioningReport = readData("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2")
    //val resultOfGetColumnsInformation = getColumnsInformation("sales")
    When("writeToKafka is invoked")
    resultOfProfiler.show()
    writeToKafka("DiskDataPartitioning",resultOfGetPartitioningReport,"TableName")
    resultOfGetPartitioningReport.show()
    //writeToKafka("filesProfiling",resultOfProfiler,"transactionId")
    //writeToKafka("datadictionarySQLTables", resultOfGetSQLTablesInfo)
    writeToKafka(topicName, resultOfGetDatabasesInfo,"name")
    writeToKafka("datadictionaryTables", resultOfGetTablesInfo,"name")
    //writeToKafka("datadictionaryDiskPartitioning",partReport1,"TableName")
    //val destinationPath = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    //val res: DataFrame = spark.read.option("header", value = true).option("value", value = true).schema(struct2).csv(destinationPath)
    writeToKafka("datadictionaryColumns", resultOfgetColumnsInformation , "name")
    //partReport1.write.saveAsTable("partReport1")
    //res.show()
    //resultOfGetTablesInfo.show()
    //writeToKafka("datadictionaryDiskPartitioning", partReport1, "TableName")
    //writeToKafka("datadictionaryColumns",resultOfgetColumnsInformation,"name")
    resultOfgetColumnsInformation.show()
    //writeToKafka("filesDatasProfiler",)
    Then("all data given should be sending to the given kafka topic")
    //todo: we should test the data in the topic (is it successfully sending)
  }
}
