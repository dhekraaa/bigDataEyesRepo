package dataDictionary

import java.io.File

import dataDictionary.DataDictionaryWithCatalogApi.readData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PartitioningDictionary {
  def writePartitioningReport(partitionedTableName: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    val partDF = spark.sql("show PARTITIONS " + partitionedTableName + " ")
    partDF.toDF("partitionKey")
      .write
      .option(key = "header", value = "true")
      .option(key = "sep", value = ",")
      .option(key = "encoding", value = "UTF-8")
      .option(key = "compresion", value = "none")
      .mode(saveMode = "OVERWRITE")
      .csv("file:///" + outputPath)
  }


  //partitioningReport ===> outputPath of writePartitioningReport
  def getPartitionsNumber(partitioningReport: DataFrame): Long = {
    partitioningReport.count()
  }

  def getPartitioningKey(partitioningReport: DataFrame): DataFrame = {
    partitioningReport.select("partitionKey")
  }

  def getPartitioningKeysV2(partitioningReport: DataFrame)(implicit spark: SparkSession): Array[String] = {
    import spark.implicits._
    val partitioningKeys: DataFrame = partitioningReport.select("partitionKey")
    var res: Array[String] = Array()
    val keyslist0 = partitioningReport.collectAsList().get(0).getString(0).split("/").map(_.trim).toList
    for (j <- 0 to keyslist0.size - 1) {
      res +:= keyslist0.toSeq.toDF().collectAsList().get(j).getString(0).replaceAll("[=\\s].*", "")

    }
    for (keysCounter <- 0 to partitioningKeys.collectAsList().size() - 1) {
      val keyslist = partitioningReport.collectAsList().get(keysCounter).getString(0).split("/").map(_.trim).toList
      var j: Int = 0
      for (i <- 0 to keyslist.size - 1) {
        if (res.contains(keyslist.toSeq.toDF("keys").collectAsList().get(i).getString(0).replaceAll("[=\\s].*", ""))) {
        }
        else {
          res +:= keyslist.toSeq.toDF("keys").collectAsList().get(i).getString(0).replaceAll("[=\\s].*", "")
        }
      }

    }

    res

  }

  def KeyChoiceRecommendation(keys: Array[String], DataSource: DataFrame): String = {
    var res: String = ""
    val keyNotRecommended = getcolumnWithHighestVariance(DataSource)
    if (keys.contains(keyNotRecommended)) {
      res = keyNotRecommended + " key can cause an over-partitioning problem, I recommend you change this key "
    }
    res
  }

  def getcolumnWithHighestVariance(DataSource: DataFrame): String = {
    val exprs2 = DataSource.columns.map((_ -> "approx_count_distinct")).toMap
    val DataVariance = DataSource.agg(exprs2)
    val structs2 = DataVariance.columns.map(
      c => struct(col(c).as("v"), lit(c).as("k"))
    )
    DataVariance.select(greatest(structs2: _*).alias("g").getItem('k')).collectAsList().get(0).getString(0)
      .replaceAll("approx_count_distinct", "")
      .replaceAll("[()]", "")
  }

  def getDirectoryFiles(partitioningRootDirectoryDataFrame: DataFrame): Seq[String] = {
    partitioningRootDirectoryDataFrame.inputFiles.toSeq
  }

  def getPartitionsSizeArrayV2(partitioningRootDirectory: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Array[Long] = {
    var res: Array[Long] = Array()
    for (keysCounter <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(keysCounter).getString(0)
      var partitionPath: String = partitioningRootDirectory + "//" + key
      val partitionData: DataFrame = readData(partitionPath)
      var files = partitionData.inputFiles
      var partSize: Long = 0
      for (fileCounter <- 0 to files.length - 1) {
        partSize = partSize + getPartitionSize(files(fileCounter))
      }
      res :+= partSize
    }
    res
  }

  def isOverPartitionedTable(partitionsSizeArray: Array[Long], seuil: Long): Boolean = {
    var partitionNumber: Int = 0
    var res: Boolean = false
    for (partSize <- 0 to partitionsSizeArray.size - 1) {
      if (partitionsSizeArray(partSize) < seuil)
        partitionNumber = partitionNumber + 1
    }
    if (partitionNumber > 2)
      res = true
    res
  }

  def getPartitionsState(partitionsSizeArray: Array[Long], seuil: Long): String = {
    var res: String = "Normal"
    if (isOverPartitionedTable(partitionsSizeArray, seuil)) {
      res = "Over-Partitioned"
    }
    res
  }


  def getOverPartitionedTableRecommendation(partitionsSizeArray: Array[Long], seuil: Long): String = {
    var tesOverPartitioning = isOverPartitionedTable(partitionsSizeArray, seuil)
    var recommendation: String = ""
    if (tesOverPartitioning == true) {
      recommendation = "Queries will perform better when you specify the partitioning key in the criteria (use the “where clause”)"
    }
    recommendation
  }

  def getFilesSizeArrayPerPartitionV2(partitioningRootDirectory: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Array[String] = {
    var res: Array[String] = Array()
    for (keysCounter <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(keysCounter).getString(0)
      var partitionPath: String = partitioningRootDirectory + "//" + key
      val partitionData: DataFrame = readData(partitionPath)
      var files = partitionData.inputFiles
      var fileSize: Seq[Long] = Seq()
      for (fileCounter <- 0 to files.length - 1) {
        // partSize = partSize + getPartitionSize(files(fileCounter))
        fileSize :+= getPartitionSize(files(fileCounter))
      }
      res :+= fileSize.mkString(", ")
    }
    res
  }

  def getFilesNumberSizePerPartitionRecommendation(FileSizeArray: Array[String], seuil: Long, partitioningKeysreport: DataFrame)(implicit spark: SparkSession): Array[String] = {
    var recommendation = ""
    var filesProbNB: Long = 0
    var recommendationArray: Array[String] = Array()
    for (partSize <- 0 to FileSizeArray.size - 1) {
      val filessizelistPerPart = FileSizeArray(partSize).split(",").map(_.trim).toList
      for (partFilesCounter <- 0 to filessizelistPerPart.size - 1) {
        if (filessizelistPerPart(partFilesCounter).toLong < seuil) {
          filesProbNB = filesProbNB + 1
        }
      }
      if (filesProbNB > 0) {
        recommendationArray :+= " You have an over-partitioning problem in: " + partitioningKeysreport.collectAsList.get(partSize).getString(0) + ", I recommend you use repartition "
      }
    }
    recommendationArray
  } //this
//Queries will perform better when you specify the partitioning key in the criteria (use the “where clause”)
  def getPartitionSizeV2(partitioningRootDirectory: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Long = {
    var partSize: Long = 0
    for (keysCounter <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(keysCounter).getString(0)
      var partitionPath: String = partitioningRootDirectory + "//" + key
      val partitionData: DataFrame = readData(partitionPath)
      var files = partitionData.inputFiles
      for (fileCounter <- 0 to files.length - 1) {
        partSize = partSize + getPartitionSize(files(fileCounter))
      }

    }
    partSize
  }

  def getFilesNumberByPartition(partitionsOutpoutFiles: DataFrame, partitionKey: String)(implicit spark: SparkSession): Long = {
    import spark.implicits._
    partitionsOutpoutFiles.where(locate(partitionKey, $"filesName") > 0).count()
  }

  def getFilesNumberByPartitionArray(partitionsOutpoutFiles: DataFrame, partitioningKeys: DataFrame)(implicit spark: SparkSession): Array[Long] = {
    var res: Array[Long] = Array()
    for (i <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(i).getString(0)
      res :+= getFilesNumberByPartition(partitionsOutpoutFiles, key)
    }
    res
  }

  def getFilesNumberPerPartitionV2(partitioningRootDirectory: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Array[Int] = {
    var res: Array[Int] = Array()
    for (keysCounter <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(keysCounter).getString(0)
      var partitionPath: String = partitioningRootDirectory + "//" + key
      val partitionData: DataFrame = readData(partitionPath)
      var files = partitionData.inputFiles
      var partFilesNb = partitionData.inputFiles.length
      res :+= partFilesNb
    }
    res
  }

  def getPartitionDirectoryPath(rootPath: String, PartirionKey: String): String = {
    rootPath + "\\" + PartirionKey
  }


  def getPartitionSize(dataPartitionDirectoryPath: String)(implicit spark: SparkSession): Long = {
    import spark.implicits._
    val dataPartitionDirectory: DataFrame = spark.read
      .option("description", value = true)
      .option("header", value = true).option("nullable", value = true)
      .option("isPartition", value = true)
      .option("isBucket", value = true)
      .option("dataType", value = true)
      .csv(dataPartitionDirectoryPath)
    dataPartitionDirectory.map(_.mkString(",").getBytes("UTF-8").length.toLong)
      .reduce(_ + _)
  }

  def getPartitionsSizeArray(dataPartitioningPath: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Array[Long] = {
    var res: Array[Long] = Array()
    for (i <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(i).getString(0)
      res :+= getPartitionSize(dataPartitioningPath + "//" + key)
    }
    res
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getPartitionFilesSize(dataPartitioningPath: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Array[Long] = {
    var res: Array[Long] = Array()
    List
    for (i <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(i).getString(0)
      var filesPart: List[File] = getListOfFiles(dataPartitioningPath + "//" + key)

      res :+= getPartitionSize(filesPart.filter(_.getName.endsWith(".parquet")).mkString(""))
    }
    res
  }

  def getTotlaPartitionsSize(dataPartitioningPath: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Long = {
    var res: Array[Long] = Array()
    List
    for (i <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(i).getString(0)
      var filesPart: List[File] = getListOfFiles(dataPartitioningPath + "//" + key)
      res :+= getPartitionSize(filesPart.filter(_.getName.endsWith(".parquet")).mkString(""))
    }
    res.sum
  }

  def getTotalPartitionSizeV2(partitioningRootDirectory: String, partitioningKeys: DataFrame)(implicit spark: SparkSession): Long = {
    var res: Long = 0
    for (keysCounter <- 0 to partitioningKeys.collectAsList().size() - 1) {
      var key = partitioningKeys.collectAsList().get(keysCounter).getString(0)
      var partitionPath: String = partitioningRootDirectory + "//" + key
      val partitionData: DataFrame = readData(partitionPath)
      var files = partitionData.inputFiles
      var partSize: Long = 0
      for (fileCounter <- 0 to files.length - 1) {
        partSize = partSize + getPartitionSize(files(fileCounter))

      }
      res = res + partSize

    }
    res
  }

  def WriteDiskPartitionsReport(records: Seq[(String, Long, Long, String, String, Long, Long, Long, String, String, String, String, String, String)], destinationPath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    records.toDF("TableName", "PartitionsTotalSize", "PartitionNumber", "PartitionsSize", "PartitionsKey", "MaxPartitionSize", "MinPartitionSize", "AvgPartitionSize", "FilesNumberPerPartition ", "FilesSizePerPartition", "PartitionRecommendation", "KeyChoiceRecommendation", "Status", "Date")
      .write
      .option(key = "header", value = "true")
      .option(key = "sep", value = ",")
      .option(key = "encoding", value = "UTF-8")
      .option(key = "compresion", value = "none")
      .mode(saveMode = "APPEND")
      .csv("file:///" + destinationPath)
  }

  def countOverPartitionedTablesProb(records: DataFrame): Float = {
    var countOverPartionedTables = 0f;
    val l = records.collectAsList().size() - 1
    for (i <- 0 to l) {
      if (records.select("Status").collectAsList().get(i).getString(0).contains("Over-Partitioned")) {
        countOverPartionedTables = countOverPartionedTables + 1
      }
    }
    countOverPartionedTables
  }

  def OverPartitionnedPercent(countOverPartitionedTablesProb: Float, records: DataFrame): Float = {
    (countOverPartitionedTablesProb / records.count().toFloat) * 100
  }

  def countkeyProblems(records: DataFrame): Long = {
    var countKeyProb: Long = 0;
    val l = records.collectAsList().size() - 1
    for (i <- 0 to l) {
      if (records.select("KeyChoiceRecommendation").collectAsList().get(i).getString(0).length > 5) {
        countKeyProb = countKeyProb + 1
      }
    }
    countKeyProb
  }

  def getMaxPartitionSize(data: Array[Long]): Long = {
    data.foldLeft((data(0))) { case ((max), e) => (math.max(max, e)) }
  }

  def getMinPartitionSize(data: Array[Long]): Long = {
    data.foldLeft((data(0))) { case ((min), e) => (math.min(min, e)) }
  }

  def getAvgPartitionSize(data: Array[Long], partitionsNumber: Long): Long = {
    var i = 0
    var sum = 0
    while ( {
      i < data.length
    }) {
      sum += data(i).toInt
      i += 1
    }
    sum / partitionsNumber.toInt
  }

}
