package dataDictionary

import java.util.Calendar
import dataDictionary.DataDictionaryWithCatalogApi.readData
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dataDictionary.PartitioningDictionary._
import persistance.KafkaProducer.writeToKafka

class PartitioningDictionarySpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val data = readData("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv")
  "WriteDiskPartitionsReport" should "give a partition report for each given record using a given output path" in {
    val data = readData("src/test/resources/sales.csv")
    data.toDF()
      .write
      .partitionBy("transactionId")
      .mode(SaveMode.Overwrite)
      .saveAsTable("PartitionedSales")
    Given("a Sequence of data partitioning information, destination path ")
    val partitionedTableName: String = "partitionedSales"
    val partitioningPath: String = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\partitionedsales"
    val outputPath: String = "C:\\\\Users\\\\kassa\\\\OneDrive\\\\Bureau\\\\DataCatalogProjectWithTDDMethodology\\\\src\\\\test\\\\resources\\\\keys"
    writePartitioningReport("partitionedSales", outputPath)
    val partitioningInfoReport: DataFrame = readData(outputPath)
    val partitionNB: Long = getPartitionsNumber(partitioningInfoReport)
    val partitioningKeys: DataFrame = getPartitioningKey(partitioningInfoReport)
    val keys: Array[String] = getPartitioningKeysV2(partitioningInfoReport)
    val keyRecommendations = KeyChoiceRecommendation(keys, data) //this*******
    val PartitioningFiles: DataFrame = readData(partitioningPath)
    val partitioningfilesInfo: DataFrame = getDirectoryFiles(PartitioningFiles).toDF("filesName")
    // val nbFilesPerPartitionArray: Array[Long] = getFilesNumberByPartitionArray(partitioningfilesInfo, partitioningKeys)
    val nbFilesPerPartitionArray: Array[Int] = getFilesNumberPerPartitionV2(partitioningPath, partitioningKeys)
    //va1l TotalSize: Long = getPartitionSize(partitioningPath)
    val totalSize: Long = getTotlaPartitionsSize(partitioningPath, partitioningKeys)
    // val partitionsSizeArray: Array[Long] = getPartitionsSizeArray(partitioningPath, partitioningKeys)
    val partitionsSizeArrayV2: Array[Long] = getPartitionsSizeArrayV2(partitioningPath, partitioningKeys) //this
    val partitioningOverPartitionedTableRecommendation = getOverPartitionedTableRecommendation(partitionsSizeArrayV2, 500) //this*******
    val partitionMaxSize: Long = getMaxPartitionSize(partitionsSizeArrayV2)
    val partitionMinSize: Long = getMinPartitionSize(partitionsSizeArrayV2)
    val avg_partitionSize: Long = getAvgPartitionSize(partitionsSizeArrayV2, partitionNB)
    //val filesSizePerPartition: String = getPartitionFilesSize(partitioningPath, partitioningKeys).toSeq.mkString(", ")
    val filesSizePerPartition: Array[String] = getFilesSizeArrayPerPartitionV2(partitioningPath, partitioningKeys)
    val NbFilesPerPartitionRecommendaion: Array[String] = getFilesNumberSizePerPartitionRecommendation(filesSizePerPartition, 500, partitioningKeys)
    val PartitioningState: String = getPartitionsState(partitionsSizeArrayV2, 500)
    val records: Seq[(String, Long, Long, String, String, Long, Long, Long, String, String, String, String, String, String)] =
      Seq((partitionedTableName, totalSize, partitionNB, partitionsSizeArrayV2.toSeq.mkString(", "), keys.toSeq.mkString("/ "), partitionMaxSize, partitionMinSize, avg_partitionSize, nbFilesPerPartitionArray.toSeq.mkString(", "), filesSizePerPartition.toSeq.mkString("/ "), partitioningOverPartitionedTableRecommendation, keyRecommendations, PartitioningState, Calendar.getInstance().getTime().toString()))
    val destinationPath = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    When("WritePartitionsReport is invoked")
    val partReport1: DataFrame = records.toDF("TableName", "PartitionsTotalSize", "PartitionNumber", "PartitionsSize", "PartitionsKey", "MaxPartitionSize", "MinPartitionSize", "AvgPartitionSize", "FilesNumberPerPartition ", "FilesSizePerPartition", "PartitionRecommendation", "KeyChoiceRecommendation", "PartitionState", "Date")
    //    writeToKafka("datadictionaryDiskPartitioning", partReport1, "TableName")
    WriteDiskPartitionsReport(records, destinationPath)
    Then("all partitioning information should be returned")
    val info = readData(destinationPath)
    info.show()
  }

  "WriteDiskPartitionsReport_Scenario2" should "give a partition report for each given record using a given output path" in {
    Given("a Sequence of data partitioning information, destination path ")
    val data = readData("src/test/resources/sales.csv")
    data.toDF()
      .write
      .partitionBy("itemId")
      .mode(SaveMode.Overwrite)
      .saveAsTable("PartitionedSalesExp2")
    val partitionedTableName = "partitionedSalesExp2"
    val partitioningPath2: String = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\PartitionedSalesExp2"
    val outputPath2: String = "C:\\\\Users\\\\kassa\\\\OneDrive\\\\Bureau\\\\DataCatalogProjectWithTDDMethodology\\\\src\\\\test\\\\resources\\\\keysExp2"
    writePartitioningReport("PartitionedSalesExp2", outputPath2)
    val partitioningInfoReport2: DataFrame = readData(outputPath2)
    val partitionNB: Long = getPartitionsNumber(partitioningInfoReport2)
    val partitioningKeys2: DataFrame = getPartitioningKey(partitioningInfoReport2)
    val keys: Array[String] = getPartitioningKeysV2(partitioningInfoReport2)
    val keyRecommendations = KeyChoiceRecommendation(keys, data) //this*******
    val PartitioningFiles2: DataFrame = readData(partitioningPath2)
    val partitioningfilesInfo2: DataFrame = getDirectoryFiles(PartitioningFiles2).toDF("filesName")
    //val nbFilesPerPartitionArray: Array[Long] = getFilesNumberByPartitionArray(partitioningfilesInfo2, partitioningKeys2)
    val nbFilesPerPartitionArray: Array[Int] = getFilesNumberPerPartitionV2(partitioningPath2, partitioningKeys2)
    //val TotalSize: Long = getPartitionSize(partitioningPath2)
    val totalSize: Long = getTotlaPartitionsSize(partitioningPath2, partitioningKeys2)
    val partitionsSizeArray: Array[Long] = getPartitionsSizeArrayV2(partitioningPath2, partitioningKeys2)
    val partitioningOverPartitionedTableRecommendation = getOverPartitionedTableRecommendation(partitionsSizeArray, 500) //this*******
    val partitionMaxSize: Long = getMaxPartitionSize(partitionsSizeArray)
    val partitionMinSize: Long = getMinPartitionSize(partitionsSizeArray)
    val avg_partitionSize: Long = getAvgPartitionSize(partitionsSizeArray, partitionNB)
    //val filesSizePerPartition: String = getPartitionFilesSize(partitioningPath2, partitioningKeys2).toSeq.mkString(", ")
    val filesSizePerPartition: Array[String] = getFilesSizeArrayPerPartitionV2(partitioningPath2, partitioningKeys2)
    val NbFilesPerPartitionRecommendaion: Array[String] = getFilesNumberSizePerPartitionRecommendation(filesSizePerPartition, 500, partitioningKeys2)
    val PartitioningState: String = getPartitionsState(partitionsSizeArray, 500)
    val records2: Seq[(String, Long, Long, String, String, Long, Long, Long, String, String, String, String, String, String)] =
      Seq((partitionedTableName, totalSize, partitionNB, partitionsSizeArray.toSeq.mkString(", "), keys.toSeq.mkString("/"), partitionMaxSize, partitionMinSize, avg_partitionSize, nbFilesPerPartitionArray.toSeq.mkString(", "), filesSizePerPartition.toSeq.mkString("/ "), partitioningOverPartitionedTableRecommendation, keyRecommendations, PartitioningState, Calendar.getInstance().getTime().toString()))
    val destinationPath2 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    When("WritePartitionsReport is invoked")
    WriteDiskPartitionsReport(records2, destinationPath2)
    Then("all partitioning information should be returned")
    val info = readData(destinationPath2)
    info.show()
  }

  "WriteDiskPartitionsReport_Scenario3" should "give a partition report for each given record using a given output path" in {
    Given("a Sequence of data partitioning information, destination path ")
    val data = readData("src/test/resources/sales.csv")
    data.toDF()
      .repartition(5)
      .write
      .partitionBy("itemId")
      .mode(SaveMode.Overwrite)
      .saveAsTable("PartitionedSalesExp3")
    val partitionedTableName = "partitionedSalesExp3"
    val partitioningPath3: String = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\PartitionedSalesExp3"
    val outputPath3: String = "C:\\\\Users\\\\kassa\\\\OneDrive\\\\Bureau\\\\DataCatalogProjectWithTDDMethodology\\\\src\\\\test\\\\resources\\\\keysExp3"
    writePartitioningReport("PartitionedSalesExp3", outputPath3)
    val partitioningInfoReport3: DataFrame = readData(outputPath3)
    val partitionNB: Long = getPartitionsNumber(partitioningInfoReport3)
    val partitioningKeys2: DataFrame = getPartitioningKey(partitioningInfoReport3)
    val PartitioningFiles2: DataFrame = readData(partitioningPath3)
    val keys: Array[String] = getPartitioningKeysV2(partitioningInfoReport3)
    val keyRecommendations = KeyChoiceRecommendation(keys, data) //this*******
    val partitioningfilesInfo2: DataFrame = getDirectoryFiles(PartitioningFiles2).toDF("filesName")
    val nbFilesPerPartitionArray: Array[Int] = getFilesNumberPerPartitionV2(partitioningPath3, partitioningKeys2)
    //val TotalSize: Long = getPartitionSize(partitioningPath2)
    val totalSize: Long = getTotalPartitionSizeV2(partitioningPath3, partitioningKeys2)
    val partitionsSizeArray: Array[Long] = getPartitionsSizeArrayV2(partitioningPath3, partitioningKeys2)
    val partitioningOverPartitionedTableRecommendation = getOverPartitionedTableRecommendation(partitionsSizeArray, 500) //this*******
    val partitionMaxSize: Long = getMaxPartitionSize(partitionsSizeArray)
    val partitionMinSize: Long = getMinPartitionSize(partitionsSizeArray)
    val avg_partitionSize: Long = getAvgPartitionSize(partitionsSizeArray, partitionNB)
    val filesSizePerPartition: Array[String] = getFilesSizeArrayPerPartitionV2(partitioningPath3, partitioningKeys2)
    val NbFilesPerPartitionRecommendaion: Array[String] = getFilesNumberSizePerPartitionRecommendation(filesSizePerPartition, 500, partitioningKeys2)
    val PartitioningState: String = getPartitionsState(partitionsSizeArray, 500)
    val records2: Seq[(String, Long, Long, String, String, Long, Long, Long, String, String, String, String, String, String)] =
      Seq((partitionedTableName, totalSize, partitionNB, partitionsSizeArray.toSeq.mkString(", "), keys.toSeq.mkString("/"), partitionMaxSize, partitionMinSize, avg_partitionSize, nbFilesPerPartitionArray.toSeq.mkString(", "), filesSizePerPartition.toSeq.mkString("/ "), partitioningOverPartitionedTableRecommendation, keyRecommendations, PartitioningState, Calendar.getInstance().getTime().toString()))
    val destinationPath2 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    When("WritePartitionsReport is invoked")
    WriteDiskPartitionsReport(records2, destinationPath2)
    Then("all partitioning information should be returned")
    val info = readData(destinationPath2)
    info.show()
  }
  "WriteDiskPartitionsReport_Scenario4" should "give a partition report for each given record using a given output path" in {
    Given("a Sequence of data partitioning information, destination path ")
    data.toDF()
      .repartition(5)
      .write
      .partitionBy("itemId", "transactionId")
      .mode(SaveMode.Overwrite)
      .saveAsTable("PartitionedSalesExp4")
    val partitionedTableName = "partitionedSalesExp4"
    val partitioningPath4: String = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\PartitionedSalesExp4"
    val outputPath4: String = "C:\\\\Users\\\\kassa\\\\OneDrive\\\\Bureau\\\\DataCatalogProjectWithTDDMethodology\\\\src\\\\test\\\\resources\\\\keysExp4"
    writePartitioningReport("PartitionedSalesExp4", outputPath4)
    val partitioningInfoReport4: DataFrame = readData(outputPath4)
    val partitionNB: Long = getPartitionsNumber(partitioningInfoReport4)
    val partitioningKeys2: DataFrame = getPartitioningKey(partitioningInfoReport4)
    val keys: Array[String] = getPartitioningKeysV2(partitioningInfoReport4)
    val keyRecommendations = KeyChoiceRecommendation(keys, data) //this*******
    val PartitioningFiles2: DataFrame = readData(partitioningPath4)
    val partitioningfilesInfo2: DataFrame = getDirectoryFiles(PartitioningFiles2).toDF("filesName")
    val nbFilesPerPartitionArray: Array[Int] = getFilesNumberPerPartitionV2(partitioningPath4, partitioningKeys2)
    //val TotalSize: Long = getPartitionSize(partitioningPath2)
    val totalSize: Long = getTotalPartitionSizeV2(partitioningPath4, partitioningKeys2)
    val partitionsSizeArray: Array[Long] = getPartitionsSizeArrayV2(partitioningPath4, partitioningKeys2)
    val partitioningOverPartitionedTableRecommendation = getOverPartitionedTableRecommendation(partitionsSizeArray, 500) //this*******

    val partitionMaxSize: Long = getMaxPartitionSize(partitionsSizeArray)
    val partitionMinSize: Long = getMinPartitionSize(partitionsSizeArray)
    val avg_partitionSize: Long = getAvgPartitionSize(partitionsSizeArray, partitionNB)
    val filesSizePerPartition: Array[String] = getFilesSizeArrayPerPartitionV2(partitioningPath4, partitioningKeys2)
    val NbFilesPerPartitionRecommendaion: Array[String] = getFilesNumberSizePerPartitionRecommendation(filesSizePerPartition, 500, partitioningKeys2)
    val PartitioningState: String = getPartitionsState(partitionsSizeArray, 500)
    val records2: Seq[(String, Long, Long, String, String, Long, Long, Long, String, String, String, String, String, String)] =
      Seq((partitionedTableName, totalSize, partitionNB, partitionsSizeArray.toSeq.mkString(", "), keys.toSeq.mkString("/"), partitionMaxSize, partitionMinSize, avg_partitionSize, nbFilesPerPartitionArray.toSeq.mkString(", "), filesSizePerPartition.toSeq.mkString("/ "), partitioningOverPartitionedTableRecommendation, keyRecommendations, PartitioningState, Calendar.getInstance().getTime().toString()))
    val destinationPath4 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    When("WritePartitionsReport is invoked")
    WriteDiskPartitionsReport(records2, destinationPath4)
    Then("all partitioning information should be returned")
    val info = readData(destinationPath4)
    info.show()
  }

  "WriteDiskPartitionsReport_Scenario5" should "give a partition report for each given record using a given output path" in {
    Given("a Sequence of data partitioning information, destination path ")
    data.toDF()
      .write
      .partitionBy("itemId", "transactionId", "amountPaid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("PartitionedSalesExp5")
    val partitionedTableName = "partitionedSalesExp5"
    val partitioningPath4: String = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\PartitionedSalesExp5"
    val outputPath4: String = "C:\\\\Users\\\\kassa\\\\OneDrive\\\\Bureau\\\\DataCatalogProjectWithTDDMethodology\\\\src\\\\test\\\\resources\\\\keysExp5"
    writePartitioningReport("PartitionedSalesExp5", outputPath4)
    val partitioningInfoReport4: DataFrame = readData(outputPath4)
    val partitionNB: Long = getPartitionsNumber(partitioningInfoReport4)
    val partitioningKeys2: DataFrame = getPartitioningKey(partitioningInfoReport4)
    val keys: Array[String] = getPartitioningKeysV2(partitioningInfoReport4)
    val keyRecommendations = KeyChoiceRecommendation(keys, data) //this*******
    val PartitioningFiles2: DataFrame = readData(partitioningPath4)
    val partitioningfilesInfo2: DataFrame = getDirectoryFiles(PartitioningFiles2).toDF("filesName")
    val nbFilesPerPartitionArray: Array[Int] = getFilesNumberPerPartitionV2(partitioningPath4, partitioningKeys2)
    //val TotalSize: Long = getPartitionSize(partitioningPath2)
    val totalSize: Long = getTotalPartitionSizeV2(partitioningPath4, partitioningKeys2)
    val partitionsSizeArray: Array[Long] = getPartitionsSizeArrayV2(partitioningPath4, partitioningKeys2)
    val partitioningOverPartitionedTableRecommendation = getOverPartitionedTableRecommendation(partitionsSizeArray, 500) //this*******
    val partitionMaxSize: Long = getMaxPartitionSize(partitionsSizeArray)
    val partitionMinSize: Long = getMinPartitionSize(partitionsSizeArray)
    val avg_partitionSize: Long = getAvgPartitionSize(partitionsSizeArray, partitionNB)
    val filesSizePerPartition: Array[String] = getFilesSizeArrayPerPartitionV2(partitioningPath4, partitioningKeys2)
    val NbFilesPerPartitionRecommendaion: Array[String] = getFilesNumberSizePerPartitionRecommendation(filesSizePerPartition, 500, partitioningKeys2)
    val PartitioningState: String = getPartitionsState(partitionsSizeArray, 500)
    val records2: Seq[(String, Long, Long, String, String, Long, Long, Long, String, String, String, String, String, String)] =
      Seq((partitionedTableName, totalSize, partitionNB, partitionsSizeArray.toSeq.mkString(", "), keys.toSeq.mkString("/ "), partitionMaxSize, partitionMinSize, avg_partitionSize, nbFilesPerPartitionArray.toSeq.mkString(", "), filesSizePerPartition.toSeq.mkString("/ "), partitioningOverPartitionedTableRecommendation, keyRecommendations, PartitioningState, Calendar.getInstance().getTime().toString()))
    val destinationPath5 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    When("WritePartitionsReport is invoked")
    WriteDiskPartitionsReport(records2, destinationPath5)
    Then("all partitioning information should be returned")

    val info = readData(destinationPath5)
    val OVPartProbs: Float = countOverPartitionedTablesProb(info)
    println("OVPartProbs =  " + OVPartProbs)
    val OVProbPercent = OverPartitionnedPercent(OVPartProbs, info)
    println("OVPartPercent =  " + OVProbPercent)

    val percentOV = lit(OVPartProbs).divide(info.count())
    println("partper = " + OVProbPercent)

    val PartKeyProbs = countOverPartitionedTablesProb(info)
    println("key =  " + PartKeyProbs)

    val dfi = info.withColumn("OverPartitionedTablesNb", lit(OVPartProbs.toInt)).withColumn("OVPercent", lit(OVProbPercent)).withColumn("KeyProbNb", lit(PartKeyProbs.toInt))
    dfi.show()
    // writeToKafka("DiskDataPartitioning",dfi,"TableName")
  }


}
