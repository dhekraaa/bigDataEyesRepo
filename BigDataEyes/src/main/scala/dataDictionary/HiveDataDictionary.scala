package dataDictionary

import org.apache.spark.sql.SparkSession

object HiveDataDictionary {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession
      .builder()
      .appName("DataFrameApp")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
  }

}
