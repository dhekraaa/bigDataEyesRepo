package dataProfiling

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import helper.Helper._


case class ColumnProfile(columnName: String
                         , totalDataSetSize: Long
                         , uniqueValues: Long
                         , emptyStringValues: Long
                         , nullValues: Long
                         , numericValues: Long
                         , maxFieldLength: Int
                        ) extends Serializable {
  lazy val percentFill: Double = calculatedPercentFill(nullValues, emptyStringValues, totalDataSetSize)
  lazy val percentNumeric: Double = calculatePercentNumeric(numericValues, totalDataSetSize)
  lazy val percentMissingValues: Double = calculatePercentMissing( nullValues, emptyStringValues,totalDataSetSize)

  def columnData: List[String] = {
    List(
      columnName
      , totalDataSetSize
      , uniqueValues
      , emptyStringValues
      , nullValues
      , percentFill
      , percentNumeric
      , percentMissingValues
      , maxFieldLength
    ).map(_.toString)
  }


  def calculatedPercentFill(nullValues: Long, emptyStringValues: Long, totalRecords: Long): Double = {
    val filledRecords = totalRecords - nullValues - emptyStringValues
    percentage(filledRecords, totalRecords)
  }

  def calculatePercentNumeric(numericValues: Long, totalRecords: Long): Double = {
    percentage(numericValues, totalRecords)
  }
  //new feature 03/11/2020
  //new feature 04/11/2020
  def calculateNullValues(dataFrame: DataFrame): Double ={
    val dataframeColumns = dataFrame.columns
    println("col1: "+ dataframeColumns(0))
    var i = 0
    var nbVM: Double = 0
    for(i <- 0 to dataframeColumns.length - 1){
      nbVM = nbVM  +  dataFrame.filter(dataFrame(dataframeColumns(i)).isNull || dataFrame(dataframeColumns(i)) === "" || dataFrame(dataframeColumns(i)).isNaN).count()
      println("nbvm = "+nbVM)
    }
    println("nbvm = "+nbVM)
    (nbVM / (dataFrame.count() * dataframeColumns.length)) * 100
  }

  def calculatePercentMissing(nullValues: Long, emptyStringValues: Long, totalRecords: Long): Double = {
    percentage(nullValues + emptyStringValues , totalRecords)
  }

  // /fin new feature

  override def toString: String = {
    List(
      columnName
      , totalDataSetSize
      , uniqueValues
      , emptyStringValues
      , nullValues
      , percentFill
      , percentNumeric
      , maxFieldLength.toString()
    ).mkString(",")
  }
}

object ColumnProfile extends Serializable {

  def calculateNullValues(dataFrame: DataFrame): Double ={
    val dataframeColumns = dataFrame.columns
    println("col1: "+ dataframeColumns(0))
    var i = 0
    var nbVM: Double = 0
    for(i <- 0 to dataframeColumns.length - 1){
      nbVM = nbVM  +  dataFrame.filter(dataFrame(dataframeColumns(i)).isNull || dataFrame(dataframeColumns(i)) === "" || dataFrame(dataframeColumns(i)).isNaN).count()
      println("nbvm = "+nbVM)
    }
    println("nbvm = "+nbVM)
    (nbVM / (dataFrame.count() * dataframeColumns.length)) * 100
  }
  def ColumnProfileFactory(df: DataFrame, columnName: String): ColumnProfile = {
    val dfColumn = df.select(columnName)
    dfColumn.cache
    val recordCount = dfColumn.count()
    val uniqueValues = dfColumn.distinct().count()
    val emptyCount = dfColumn.withColumn("isEmpty", (col(columnName))).filter(col("isEmpty") === true).collectAsList()
    //.count //.count
    val d = dfColumn.select(columnName).collectAsList()
    var sumOfEmptCol = 0
    for (i <- 0 to d.size() - 1) {
      if (d.get(i).getString(0) == "") {
        sumOfEmptCol = sumOfEmptCol + 1
      }
    }
    def isAllDigits(x: String) = x forall Character.isDigit

    var sumOfNumericValues = 0
    for (i <- 0 to d.size() - 1) {
      if (isAllDigits(d.get(0).getString(0))) {
        sumOfNumericValues = sumOfNumericValues + 1
      }
    }
    val nullCount = dfColumn.withColumn("isNull", col(columnName).isNull).filter(col("isNull")).count()
    //  val numericCount = dfColumn.withColumn("isNumeric", udfIsNumeric(col(columnName))).filter(col("isNumeric") === true).count
    val maxFieldLength = dfColumn.
      withColumn("fieldLen", udfFieldLen(col(columnName))).
      agg(max(col("fieldLen"))).collect()(0)(0).toString.toInt
    new ColumnProfile(columnName, recordCount, uniqueValues, sumOfEmptCol, nullCount, sumOfNumericValues, maxFieldLength) //emptyCount, nullCount, numericCount,
  }


}
