package dataProfiling

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProfilingSQl {
  val jdbcUsername: String = "root"
  val jdbcPassword: String = ""

  def SqlStatistics()(implicit spark: SparkSession, jdbcUrl: String): DataFrame = {
    spark.sqlContext.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("query", " select TABLE_SCHEMA as databaseName, TABLE_NAME, COLUMN_NAME, NON_UNIQUE, INDEX_NAME," +
        " SEQ_IN_INDEX as sequenceIndex, COLLATION, CARDINALITY, SUB_PART, PACKED, NULLABLE, INDEX_TYPE from INFORMATION_SCHEMA.STATISTICS " +
        "where table_schema not in ('information_schema','mysql', 'performance_schema','sys') ")
      .load()
  }

  def sqlDataProfiling()(implicit spark: SparkSession, jdbcUrl: String): DataFrame = {
    spark.sqlContext.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("query", "  select TABLE_SCHEMA as databaseName,  TABLE_NAME as tableName," +
        "ROW_FORMAT as rowFormat, TABLE_ROWS as rowsNumber, AVG_ROW_LENGTH , MAX_DATA_LENGTH," +
        " INDEX_LENGTH, DATA_FREE, TABLE_COLLATION as collationType, CHECKSUM, MAX_INDEX_LENGTH from information_schema.tables " +
        " WHERE table_schema not in ('information_schema','mysql', 'performance_schema','sys', 'phpmyadmin') ")
      .load()
  }
  def profilingColumns()(implicit spark: SparkSession, jdbcUrl: String): DataFrame = {
    spark.sqlContext.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("query", " SELECT  TABLE_SCHEMA as databaseName, TABLE_NAME as tableName, COLUMN_NAME as columnName, " +
        "ORDINAL_POSITION as ordinalPosition, CHARACTER_OCTET_LENGTH as characterLength, NUMERIC_PRECISION as numericPrecision, " +
        "NUMERIC_SCALE as numericScale, DATETIME_PRECISION as datetimePrecision, CHARACTER_SET_NAME as characterType," +
        " COLLATION_NAME as collactionType" +
        "CHARACTER_MAXIMUM_LENGTH as maxLength FROM information_schema.columns")
      .load()
  }
  def profilingFiles()(implicit spark: SparkSession, jdbcUrl: String): DataFrame = {
    spark.sqlContext.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("query", " select , FILE_ID, FILE_NAME, FILE_TYPE, TABLESPACE_NAME, TABLE_SCHEMA, TABLE_NAME, " +
        " LOGFILE_GROUP_NUMBER, ENGINE, DELETED_ROWS, UPDATE_COUNT, FREE_EXTENTS, TOTAL_EXTENTS, EXTENT_SIZE, INITIAL_SIZE," +
        " MAXIMUM_SIZE, AUTOEXTEND_SIZE, TRANSACTION_COUNTER, VERSION, ROW_FORMAT, " +
        " AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, STATUS " +
        " + from INFORMATION_SCHEMA.FILES where table_schema not in ('information_schema','mysql', 'performance_schema','sys') ")
      .load()
  }

  //todo: calcul number of update time and check time of data

}
