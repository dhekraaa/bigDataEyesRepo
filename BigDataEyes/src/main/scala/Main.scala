
import dataProfiling.DataProfiling.DataProfiledFormer
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import persistance.KafkaProducer.writeToKafka

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("DataCatalog")
      .master("local[*]")
      .getOrCreate()
    println("dhekra")
    val data2: DataFrame = spark.read.option("header", true).csv(args(0))
  data2.show()
   // val data2Former = DataProfiledFormer(data2)
   // data2Former.show()
    //val data: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Costs.csv")
    //val data3Former = DataProfiledFormer(data)

  // data3Former.show()
    //writeToKafka("fileDataProfilerTopic", data3Former.withColumn("SourceName", lit("Costs")), "ColumnName")
   // Thread.sleep(10000000)
  }

  
}
