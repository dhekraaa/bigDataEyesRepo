package dataProfiling

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class ColumnProfileSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("DataCatalog")
    .getOrCreate()
import spark.implicits._

  def baseballDf: DataFrame = {
    Seq(
      ("Mets", "1986", "New York"),
      ("Yankees", "2009", "New York"),
      ("Cubs", "2016", "Chicago"),
      ("Cubs", "2005", "Chicago"),
      ("Nationals", "", "Washington")
    ).toDF("team", "lastChampionship", "city")
  }

}


