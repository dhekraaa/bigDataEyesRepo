package persistance

import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Try

case class Click(prev: String, curr: String, link: String, n: Long)

class KafkaConsumerSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  val spark = SparkSession
    .builder()
    .appName("rules-test")
    .master("local[*]")
    .getOrCreate()

  val struct: StructType =
    StructType(
      StructField("timestamp", StringType, false) ::
        StructField("transactionId", StringType, true) ::
        StructField("customerId", StringType, false) ::
        StructField("itemId", StringType, false) ::
        StructField("amountPaid", StringType, false) :: Nil)

  val data = spark.readStream.schema(struct).csv("src/test/resources/sales.csv")


  def parseVal(x: Array[Byte]): Option[Click] = {
    val split: Array[String] = new Predef.String(x).split("\\t")
    if (split.length == 4) {
      Try(Click(split(0), split(1), split(2), split(3).toLong)).toOption
    } else
      None
  }

  val dataReadForTopicTest: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "DataCatalogTopic")
    .option("startingOffsets", "earliest")
    .load()
  //.selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value","timestamp")

  import spark.implicits._

  val df1 = dataReadForTopicTest.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
    .select(from_json($"value", struct).as("data"), $"timestamp")
    .select("data.*", "timestamp")

  df1.writeStream
    .format("console")
    .option("truncate", "false")
    .start()
  println("messages")

  /*val messages: Any = dataReadForTopicTest
    .select("value").as[Array[Byte]]
    .flatMap(x => parseVal(x))
    .groupBy("curr").agg(Map("n" -> "sum"))
    .sort($"sum(n)"
      .desc)
*/
  dataReadForTopicTest.writeStream.format("console").start()

  val d = dataReadForTopicTest.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
  d.writeStream.queryName("temp").format("memory").start()
  spark.sql("select * from temp limit 10").show()

}
