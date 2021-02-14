package dataDictionary

import java.nio.file.attribute.FileTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FilesDictionary {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("DataFrameApp")
      .config("spark.master", "local[*]")
      .getOrCreate()

  def getFileProprety(filePath: String): DataFrame = {
    import spark.implicits._
    //var = Seq()
    val csvFileDF = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(filePath)

    import java.nio.file.Paths
    val fileName = Paths.get(filePath).getFileName
    val extension = fileName.toString.split("\\.").last
    val name: String = fileName.toString.split("\\.")(0)
    val linesNB: Long = csvFileDF.count()
    val columnCount: Int = csvFileDF.columns.length
    val SrcURI: String = csvFileDF.inputFiles.mkString

    val res: Seq[(String, String, Int, Long ,String)] = Seq((name, extension,columnCount,linesNB, SrcURI))
    res.toDF("FileName", "FileType","ColumnNumber","LinesNumber","URI").withColumn("creation_date",current_date())

  }

}
