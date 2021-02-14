package dashboard

import org.apache.spark.sql.{DataFrame, SparkSession}

object Dashboard {

  def getTablesNumber()(implicit spark: SparkSession): BigInt = {
    spark.catalog.listTables().count
  }

  def getDBNumber()(implicit spark: SparkSession): Integer = {
    spark.catalog.listDatabases().count().toInt
  }

  def getFilesNumber(path: String)(implicit spark: SparkSession): BigInt = {
    val files: Array[String] = spark.read.csv(path).inputFiles
    files.size
  }

  def getAllFilesNB(path: String, filesDirectory: Array[String])(implicit spark: SparkSession): BigInt = {
    var filesNB: BigInt = 0

    import spark.implicits._
    for (i <- 0 to filesDirectory.length - 1) {
      filesNB = filesNB + getFilesNumber(path + filesDirectory.toSeq.toDF().collectAsList().get(i).getString(0))

    }
    filesNB

  }

  def getReportsNumber(reportsDirectory: Array[String],path:String)(implicit spark: SparkSession): BigInt = {
    import spark.implicits._
    var res: BigInt = 0
    for (i <- 0 to reportsDirectory.length - 1) {
      res = res + getFilesNumber(path + reportsDirectory.toSeq.toDF().collectAsList().get(i).getString(0))

    }
    res
  }

  def getDashboardReport(reportData:Seq[(Integer,BigInt,BigInt,BigInt)])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    reportData.toDF("DBNB","TablesNB","FilesNB","ReportNB")
  }

}
