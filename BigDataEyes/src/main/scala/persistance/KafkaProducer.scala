package persistance

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaProducer {

  val hostNameAndPort: String = "localhost:9092"
  val serializerKey: String = ""
  val serializerValue: String = ""
  val checkPointValue: String = "C:/kafka_2.12-2.5.0/checkPoint"

  def configureKafka(): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", hostNameAndPort)
    props.put("key.serializer", serializerKey)
    props.put("value.deserializer", serializerValue)
    props
  }
//todo: parametrize key an then delete the other kafka writer method
  def writeToKafka(topic: String, data: DataFrame, key: String): Unit = {
    data
      .selectExpr("CAST("+key+"  AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", hostNameAndPort)
      .option("checkpointLocation", checkPointValue)
      .option("topic", topic)
      .save()
  }
}
