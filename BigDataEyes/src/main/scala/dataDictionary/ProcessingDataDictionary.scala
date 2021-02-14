package dataDictionary

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.spark_partition_id
object ProcessingDataDictionary {
  def getPartitionsNumber(data: DataFrame): Long = {
    data
      .rdd.getNumPartitions
  }

  def getPartitionner(data: DataFrame): String = {
    var result: String = ""
    val partitionner: String = data.rdd.partitioner.toString
    if (partitionner == "None")
      result = "Default Spark Partitioner"
    else
      result = partitionner
    result
  }

  def getRDDOfDataFrame(data: DataFrame): RDD[String] = {
    data.rdd.map(_.toString())
  }

  def getFileSize(rdd: RDD[String]): Long = {
    rdd.map(_.mkString(",").getBytes("UTF-8").length.toLong)
      .reduce(_ + _) //add the sizes together
  }

  def getMaxRecordsSizePerPartition(rdd: RDD[String]): Long = {
    rdd.map(_.mkString(",").getBytes("UTF-8").length.toLong)
      .max()
  }

  def getTotalSize(partSize: Long, NbPart: Long): Double = {
    partSize * NbPart
  }


  def getMinRecordsSizePerPartition(rdd: RDD[String]): Long = {
    rdd.map(_.mkString(",").getBytes("UTF-8").length.toLong)
      .min()
  }

  def getNumberOfRecordsPerPartition(data: DataFrame): Seq[Int] = {
    data.rdd.mapPartitions(iter => Array(iter.size).iterator, true).collect().toSeq
  }

  def getRecordSize(rdd: RDD[String]): Seq[Long] = {
    rdd.map(_.mkString(",").getBytes("UTF-8").length.toLong).toLocalIterator.toSeq
  }

  def getPartitionSize(data: DataFrame): Array[Long] = {
    var result: Array[Long] = Array()
    val recordsNumberPerPartition = getNumberOfRecordsPerPartition(data)
    val dataRDD = getRDDOfDataFrame(data)
    val recordsSize = getRecordSize(dataRDD)
    val recordsArraysize = recordsNumberPerPartition.size - 1

    for (i <- 0 to recordsArraysize) {
      var j = i
      var sum: Long = 0
      while (j < recordsNumberPerPartition(i)) {
        sum += recordsSize(j)
        j += 1
      }
      println(sum)
      result :+= sum
    }
    result
  }

  def recordsToDF(records: Seq[(Int,Long, Long, String, Long, Long, Long, String, String, Long, Long, String, String, String, String,String,String)])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    records
      .toDF("id","TotalSize", "PartitionsNumber", "PartitionSize", "MaxPartitionsSize", "MinPartitionsSize",
        "AvgPartitionsSize", "RecordsNumberPerPartition", "RecordsSize", "MaxRecordsSize", "MinRecordsSize", "AvgRecordsSize",
        "Partitioner", "Date", "STATE","PartSizeRecommendation","PartStateRecommendation") //.withColumn("id", spark_partition_id())
  }

  def WritePartitionsReport(records: DataFrame, destinationPath: String): Unit = {
    records.write
      .option(key = "header", value = "true")
      .option(key = "sep", value = ",")
      .option(key = "encoding", value = "UTF-8")
      .option(key = "compresion", value = "none")
      .mode(saveMode = "APPEND")
      .csv(destinationPath)

  }


}
