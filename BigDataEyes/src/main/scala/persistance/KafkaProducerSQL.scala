package persistance

import org.apache.spark.sql.DataFrame
import persistance.KafkaProducer.{checkPointValue, hostNameAndPort}

object KafkaProducerSQL {
  def writeSQLMetadataToKafka(topic: String, data: DataFrame, key: String): Unit = {
    data
      .selectExpr("CAST("+key+" AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", hostNameAndPort)
      .option("checkpointLocation", checkPointValue)
      .option("topic", topic)
      .save()
  }

}
