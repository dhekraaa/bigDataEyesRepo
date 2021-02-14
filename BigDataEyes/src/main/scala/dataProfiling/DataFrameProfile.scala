package dataProfiling

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class DataFrameProfile(data: DataFrame){
  data.cache

  lazy val spark = SparkSession.builder().getOrCreate()

  val columnProfiles  : List[ColumnProfile] =
    for (c <- data.columns.toList)
      yield ColumnProfile.ColumnProfileFactory(data,c)
  val header : List[String] = List("ColumnName","RecordCount", "UniqueValues", "EmptyStrings" ,"NullValues", "PercentFill", "PercentNumeric","PercentMissing", "MaxLength")
  def toDataFrame : DataFrame = {
    def dfFromListWithHeader(data: List[List[String]], header: String) : DataFrame = {
      val rows = data.map{x => Row(x:_*)}
      val rdd = spark.sparkContext.parallelize(rows)
      val schema = StructType(header.split(",").
        map(fieldName => StructField(fieldName, StringType, true)))
      spark.sqlContext.createDataFrame(rdd, schema)
    }
    val data = columnProfiles.map(_.columnData)
    dfFromListWithHeader(data,header.mkString(","))
  }

}
