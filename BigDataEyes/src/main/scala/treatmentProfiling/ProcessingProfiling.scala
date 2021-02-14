package treatmentProfiling

import org.apache.spark.sql.{DataFrame, SparkSession}

object ProcessingProfiling {
  val jdbcUsername: String = "root"
  val jdbcPassword: String = ""
  def getAllThreadsExecutingWithinTheServer()(implicit spark: SparkSession, jdbcUrl: String): DataFrame = {
    spark.sqlContext.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("query", " select * " +
        " from INFORMATION_SCHEMA.PROCESSLIST where table_schema not in ('information_schema','mysql', 'performance_schema','sys') ")
      .load()
  }
  def getAllTrigersProfiler()(implicit spark: SparkSession, jdbcUrl: String): DataFrame = {
    spark.sqlContext.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("query", " select TRIGGER_CATALOG, TRIGGER_SCHEMA, TRIGGER_NAME, ACTION_ORDER, ACTION_CONDITION, ACTION_STATEMENT, ACTION_ORIENTATION, " +
        "ACTION_TIMING, ACTION_REFERENCE_OLD_TABLE, ACTION_REFERENCE_OLD_ROW, ACTION_REFERENCE_NEW_ROW," +
        " CREATED, SQL_MODE, DEFINER from INFORMATION_SCHEMA.TRIGGERS")
      .load()
  }

}
