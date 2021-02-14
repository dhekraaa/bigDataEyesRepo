package dataProfiling

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameQuality {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("DataFrameApp")
      .config("spark.master", "local[*]")
      .getOrCreate()

  def calculateMissingValuesNb(dataFrame: DataFrame): Long ={
    val dataframeColumns = dataFrame.columns
    println("col1: "+ dataframeColumns(0))
    var i = 0
    var nbVM: Long = 0
    for(i <- 0 to dataframeColumns.length - 1){
        nbVM = nbVM  +  dataFrame.filter(dataFrame(dataframeColumns(i)).isNull || dataFrame(dataframeColumns(i)) === "" || dataFrame(dataframeColumns(i)).isNaN).count()
      println("nbvm = "+nbVM)
    }
    println("nbvm = "+nbVM)
    nbVM
  }

  def calculateMissingValuesPercentage(dataFrame: DataFrame, MissingValuesNumber: Double): Double ={
    val dataframeColumns = dataFrame.columns
    ((MissingValuesNumber / (dataFrame.count() * dataframeColumns.length)) * 100)
  }

  def calculateLinesReplicationPercentage(dataFrame: DataFrame): Double = {
    val uniqueValues = dataFrame.distinct().count()
    println("uniqueValues " + uniqueValues)
    val totalNb = dataFrame.count().toDouble
    println("total "+ totalNb)
    println("res:  "+ (uniqueValues / totalNb) *100 )
    println("test replicated values: " + (((totalNb - uniqueValues)/totalNb)* 100))
    (((totalNb - uniqueValues)/totalNb)* 100)
  }

  def calculateLinesReplicationNb(data: DataFrame): Long = {
    data.count() - data.distinct().count()
  }

  def fileNameAndType(data: DataFrame):String ={

    data.inputFiles.mkString(",").split("/").map(_.trim).toList.last
  }
  def filePath_(data: DataFrame):String ={
    data.inputFiles(0)
  }
  def qualityProfiling(fileName: String, count: Double, MissingValuesNb: Int, missingValuesPercent: Double,replicationValuesNb: Int,replicationValuesPercent: Double, filePath: String):DataFrame = {
    var records: Seq[(String,Int,Long,String,Long,String,String)] = Seq((fileName, count.toInt,MissingValuesNb.toInt,missingValuesPercent.formatted("%.2f") + " %",replicationValuesNb.toInt,replicationValuesPercent.formatted("%.2f")+ " %",filePath))
    import spark.implicits._
    records.toDF("FileName","count","MissingValuesNumber","missingValuesPercent","replicationValuesNb","replicationValuesPercent", "FilePath")
  }

}
