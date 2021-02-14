package dataDictionary

import java.util.Calendar

import persistance.KafkaProducer._
import dataDictionary.ProcessingDataDictionary._
import dataDictionary.DataDictionaryWithCatalogApi.readData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import dataProfiling.ProcessingPartitioningProfiling._

class ProcessingDataDictionarySpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[2]")
    .getOrCreate()
  val data = readData("src/test/resources/sales.csv")
  val resultOfGetPartitionsNumber = data.rdd.getNumPartitions

  "getPartitionsNumber" should "return the number of partitions of a given data " in {
    Given("dataframe of data")
    When("getPartitionsNumber is invoked")
    val partNumb = getPartitionsNumber(data)
    Then("the number of partitions should be returned ")
    partNumb should equal(resultOfGetPartitionsNumber)
  }
  "WritePartitionsRepport" should "write a partition report for each given record and save it using destination path" in {
    Given("dataframe of data, destination path ")
    val rdd: RDD[String] = getRDDOfDataFrame(data)
    val id = data.rdd.zipWithIndex().id
    //val id = rdd.zipWithUniqueId().toString()
    val totalSize = getFileSize(rdd)
    val partNumber: Long = getPartitionsNumber(data)
    val partitionsSize: Array[Long] = getPartitionSize(data)
    val maxPartitionSize: Long = getMaxPartitionSize(partitionsSize)
    val minPartitionSize: Long = getMinPartitionSize(partitionsSize)
    val avg_partition_size: Long = getAvgPartitionSize(partitionsSize, partNumber)
    val recordsNumber: Seq[Int] = getNumberOfRecordsPerPartition(data) //.mkString(", ")
    val recordSize: Array[Long] = getRecordSize(rdd).toArray //.toSeq.mkString(", ")
    val MaxRecordsSize: Long = getMaxRecordsSizePerPartition(rdd)
    val MinRecordsSize: Long = getMinRecordsSizePerPartition(rdd)
    val avg_Records_Size: Array[Long] = getAvgRecordsSize(data)
    val partitioner: String = getPartitionner(data)
    val statuts: String = getPartitioningState(partNumber, 2)

    val StateRecommendation: String = getPartitoinsStateRecommendadtion(partNumber, 2)
    val PartSizeRecommendation: String = getPartitionsSizeRecommendation(avg_partition_size, 900)

    val records: Seq[(Int,Long, Long, String, Long, Long, Long, String, String, Long, Long, String, String, String, String, String, String)] =
      Seq((id,totalSize, partNumber, partitionsSize.toSeq.mkString(", "), maxPartitionSize, minPartitionSize, avg_partition_size,
        recordsNumber.toSeq.mkString(", "), recordSize.toSeq.mkString(", "), MaxRecordsSize, MinRecordsSize,
        avg_Records_Size.toSeq.mkString(", "), partitioner, Calendar.getInstance().getTime().toString(), statuts, PartSizeRecommendation, StateRecommendation))

    val path: String = "file:///C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dataProcessingPartitionsReportV2"
    When("writePartitionRepport is invoked")
    val dataReport: DataFrame = recordsToDF(records)
    dataReport.show()
    WritePartitionsReport(dataReport,path)
    Then("all partitioning information should be returned")
    //val df = readData(path).toDF()
    println("ID = " + id)
    // df.createTempView("partitionReport")
    //df.show()
  }
  "WritePartitionsRepport" should "give a partition report for each given record" in {
    import spark.implicits._
    val nums = List("1", "2", "3", "5846", "78", "1", "78")
    val rdd2 = spark.sparkContext.parallelize(nums)
    val id = rdd2.zipWithIndex().id
    val d: DataFrame = nums.toDF()

    val totalSize = getFileSize(rdd2)
    val partitionNumber: Long = getPartitionsNumber(d)
    println("number of partitions ==== " + partitionNumber)
    val partitionsSize: Array[Long] = getPartitionSize(d)
    val maxPartitionSize: Long = getMaxPartitionSize(partitionsSize)
    val minPartitionSize: Long = getMinPartitionSize(partitionsSize)
    val avg_partition_size: Long = getAvgPartitionSize(partitionsSize, partitionNumber)
    val recordsNumber: Seq[Int] = getNumberOfRecordsPerPartition(d) //.mkString(", ")
    val recordSize: Array[Long] = getRecordSize(rdd2).toArray //.toSeq.mkString(", ")
    val MaxRecordsSize: Long = getMaxRecordsSizePerPartition(rdd2)
    val MinRecordsSize: Long = getMinRecordsSizePerPartition(rdd2)
    val avg_Records_Size: Array[Long] = getAvgRecordsSize(d)
    val partitioner: String = getPartitionner(d)
    val statuts: String = getPartitioningState(partitionNumber, 1)
    val StateRecommendation: String = getPartitoinsStateRecommendadtion(partitionNumber, 1)
    val PartSizeRecommendation: String = getPartitionsSizeRecommendation(avg_partition_size, 900)
    val records: Seq[(Int,Long, Long, String, Long, Long, Long, String, String, Long, Long, String, String, String, String, String, String)] =
      Seq((id,totalSize, partitionNumber, partitionsSize.toSeq.mkString(", "), maxPartitionSize, minPartitionSize, avg_partition_size,
        recordsNumber.toSeq.mkString(", "), recordSize.toSeq.mkString(", "), MaxRecordsSize, MinRecordsSize, avg_Records_Size.toSeq.mkString(", "),
        partitioner, Calendar.getInstance().getTime().toString(), statuts, PartSizeRecommendation, StateRecommendation))

    val path: String = "file:///C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dataProcessingPartitionsReportV2"
    When("writePartitionRepport is invoked")
    val dataReport: DataFrame = recordsToDF(records)
    WritePartitionsReport(dataReport, path)
    Then("all partitioning information should be returned")
    //val df = readData(path).toDF()
    //df.createTempView("partitionReportWithParallelismMode")
   // df.show()


  }
  "WritePartitionsRepportSc3" should "write a partition report for each given record and save it using destination path" in {
    Given("dataframe of data, destination path ")
   val data = readData("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\partitionedsales\\transactionId=111\\part-00000-c13bc85f-2cc2-48b8-8a4d-9af9ea287f03.c000.snappy.parquet")
    val id = data.rdd.zipWithIndex().id
    val rdd: RDD[String] = getRDDOfDataFrame(data)
    val totalSize = getFileSize(rdd)
    val partNumber: Long = getPartitionsNumber(data)
    val partitionsSize: Array[Long] = getPartitionSize(data)
    val maxPartitionSize: Long = getMaxPartitionSize(partitionsSize)
    val minPartitionSize: Long = getMinPartitionSize(partitionsSize)
    val avg_partition_size: Long = getAvgPartitionSize(partitionsSize, partNumber)
    val recordsNumber: Seq[Int] = getNumberOfRecordsPerPartition(data) //.mkString(", ")
    val recordSize: Array[Long] = getRecordSize(rdd).toArray //.toSeq.mkString(", ")
    val MaxRecordsSize: Long = getMaxRecordsSizePerPartition(rdd)
    val MinRecordsSize: Long = getMinRecordsSizePerPartition(rdd)
    val avg_Records_Size: Array[Long] = getAvgRecordsSize(data)
    val partitioner: String = getPartitionner(data)
    val statuts: String = getPartitioningState(partNumber, 1)

    val StateRecommendation: String = getPartitoinsStateRecommendadtion(partNumber, 1)
    val PartSizeRecommendation: String = getPartitionsSizeRecommendation(avg_partition_size, 900)

    val records: Seq[(Int,Long, Long, String, Long, Long, Long, String, String, Long, Long, String, String, String, String, String, String)] =
      Seq((id,totalSize, partNumber, partitionsSize.toSeq.mkString(", "), maxPartitionSize, minPartitionSize, avg_partition_size,
        recordsNumber.toSeq.mkString(", "), recordSize.toSeq.mkString(", "), MaxRecordsSize, MinRecordsSize,
        avg_Records_Size.toSeq.mkString(", "), partitioner, Calendar.getInstance().getTime().toString(), statuts, PartSizeRecommendation, StateRecommendation))

    val path: String = "file:///C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dataProcessingPartitionsReportV2"
    When("writePartitionRepport is invoked")
    val dataReport: DataFrame = recordsToDF(records)
    WritePartitionsReport(dataReport, path)
    Then("all partitioning information should be returned")
    val df = readData(path).toDF()
    writeToKafka("processingDataPartitioningReport",df,"TotalSize")
    // df.createTempView("partitionReport")
    df.show()
  }

}
