package dashboard

import java.io.File

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dashboard.Dashboard._
import dataDictionary.DataDictionaryWithCatalogApi.{getColumnsInformation, getTablesInformation, readData}
import persistance.KafkaProducer._
import persistance.DiskWriter._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DashboardSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[2]")
    .getOrCreate()
  spark.sql("CREATE TABLE boxes (width INT, length INT, height INT) USING CSV")
  spark.sql("CREATE TABLE sale (employee INT, name String, height INT) USING CSV")

  spark.sql("CREATE TABLE my_table (name STRING, age INT, hair_color STRING)" +
    "  USING PARQUET OPTIONS(INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'," +
    "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'," +
    "  SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')" +
    "  PARTITIONED BY (hair_color)" +
    "  TBLPROPERTIES ('status'='staging', 'owner'='andrew')")
  getTablesInformation().show()

  "" should "" in {
    val filesDirectory: Array[String] = Array("partitionedsales", "partitionedsalesexp2", "partitionedsalesexp3", "partitionedsalesexp4", "partitionedsalesexp5")
    val reportsDirectories: Array[String] = Array("diskPartitioningReportV2") //, "dataFrameProfilingReports", "qualityProfilingReports")
    val dbNB: Integer = getDBNumber()
    val tablesNb: BigInt = getTablesNumber()
    val filesNB: BigInt = getAllFilesNB("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\", filesDirectory)
    val reportNB = getReportsNumber(reportsDirectories,"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\")
    println("db nb : " + dbNB + " | tables NB : " + tablesNb +
      " | files nb: " + filesNB + " reportNB"+ reportNB)
    val seq : Seq[(Integer,BigInt,BigInt,BigInt)] = Seq((dbNB,tablesNb,filesNB,reportNB))
    val dfDashboard: DataFrame = getDashboardReport(seq)
    dfDashboard.show()
   // writeToDisk(dfDashboard,"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport")
    val d = spark.read.option("header", value = true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport\\part-00000-6dc192ee-e628-4464-a3ea-2a497e73cad5-c000.csv")

    //readData("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport")
   writeToKafka("DashboardTopic",d,"DBNB")
    //"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\"
    val resultOfGetTablesInfo = getTablesInformation("default")
    val resultOfgetColumnsInformation = getColumnsInformation("sales")
    writeToKafka("datadictionaryTables", resultOfGetTablesInfo,"name")
    writeToKafka("datadictionaryColumns", resultOfgetColumnsInformation , "name")

  }

}
