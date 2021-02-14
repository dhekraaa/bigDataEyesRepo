package persistance

import java.sql.Struct
import java.util.Properties

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KafkaConsumer {
  def readDataFromKafkaTopic(TopicName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", TopicName)
      .option("startingOffsets", "earliest")
      .load()

  }
  import java.sql.Connection
  import java.sql.DriverManager
  val jdbcHostname = "localhost"
  val jdbcPort = 3306
  val jdbcDatabase = "banque"
  val jdbcUsername = "root"
  val jdbcPassword = ""
  val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

  val connectionProperties = new Properties()

  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")
  def main(args: Array[String]): Unit = {

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    println(connection.getMetaData())
  }

  def parsingKafkaData(StreamingDataRead: DataFrame, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    StreamingDataRead.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", schema).as("data"), $"timestamp")
      .select("data.*", "timestamp")
  }

}
