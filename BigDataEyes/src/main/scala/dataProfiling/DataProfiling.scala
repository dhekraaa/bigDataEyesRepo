package dataProfiling

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProfiling {
  var dataFrame: DataFrame = _

  //todo : data should be read with format and not csv
  def readData(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.option("header", value = true).option("description", value = true).csv(path)
  }

  def dataProfiler(data: DataFrame): DataFrame = {
    data.describe(data.columns: _*)
  }

  def DataProfiledFormer(dataToProfile: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    var res : Seq[(String,String,String,String,String,String, String)] = Seq()
    val result: List[Seq[(String,String,String,String,String,String)]] = List() //res.toDF("ColN","count","mean","stddev","min","max")
    val Cols: Array[String] = dataToProfile.columns
    val dataProfiled: DataFrame =dataProfiler(dataToProfile)
    for( i <- 0 to Cols.size -1 ){
      var line = dataProfiled.select(Cols(i)).collectAsList()
      var s :Seq[(String,String,String,String,String,String,String)] =Seq((Cols(i), line.get(0).getString(0),line.get(1).getString(0),line.get(2).getString(0),line.get(3).getString(0), line.get(4).getString(0),dataToProfile.inputFiles.toSeq.toDF().collectAsList().get(0).getString(0)))
     s.toDF("ColN","count","mean","stddev","min","max","SrcN").show()
      res ++= s

    }
    res.toDF("ColumnName","count","mean","stddev","min","max","SrcName")

  }

  def getFilesToProfile(pathIn: String, pathOut: String)(implicit spark: SparkSession): Array[String] = {
    val DataToProfile = readData(pathIn)
    DataToProfile.inputFiles
  }
  //def getMax():
}
