package persistance

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaProducerSQLSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[*]")
    .getOrCreate()

  val jdbcHostname = "localhost"
  val jdbcPort = 3306
  val jdbcDatabase = "banque"
  implicit val jdbcUrl: String = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
/*  "writeSQLMetadataToKafka" should "write a given dataFrame to a given kafka topic " in {
    Given("topic name, data frame to send and key name")
    val tablesTopic: String = "datadictionarySQLTables"
    val resultOfGetSQLDatabasesInfo = getDatabasesTablesDetails_SQL(jdbcUrl)
    val keyNameForFilesInfo: String = "FILE_ID"
    val TopicForFilesINFO: String = "datadictionaryFilesDetails"
    val ColumnsTopic = "datadictionarySQLDBColumns"
    val resultOfDBColumns = getDataBaseColumnsDetails(jdbcUrl)
    val resultOfGlobalDatabasesInfo = getGlobalDatabasesInformations(jdbcUrl)
    val resultOfgetFilesDetails = getFilesDetails(jdbcUrl)
    val resultOfConstraints = getAllConstraintsDetails(jdbcUrl)
    val resforSqProfiling = SqlStatistics()
    val resForgetAllTrigersDetails = getAllTrigersDetails(jdbcUrl)
    //resForgetAllTrigersDetails.show()
    val resForgetAllTrigersProfiler = getAllTrigersProfiler()
    //resForgetAllTrigersProfiler.show()
    val resForgetAllConstraintsDetails = getAllConstraintsDetails(jdbcUrl)
    resForgetAllConstraintsDetails.show()
    val keyForAllConstraintsDetails = "tableName"
    val topicForAllConstraintsDetails = "datadictionarySQLAllConstraints"
    val resOfgetUserPrivilegesDetails = getUserPrivilegesDetails(jdbcUrl)
    // resOfgetUserPrivilegesDetails.show()
    val resOfgetObjectsPrivilegesDetails = getAllPrivilegesDetails(jdbcUrl)

    // resOfgetObjectsPrivilegesDetails.show()
    //resforSqProfiling.show()
    When("writeToKafka is invoked")
    writeSQLMetadataToKafka("datadictionarySQLAllPrivileges", resOfgetObjectsPrivilegesDetails, "tableName")
    writeSQLMetadataToKafka(topicForAllConstraintsDetails, resForgetAllConstraintsDetails, keyForAllConstraintsDetails)
    //writeSQLMetadataToKafka(TopicForFilesINFO,resultOfgetFilesDetails,keyNameForFilesInfo)
    Then("all data given should be sending to the given kafka topic")
    //resultOfConstraints.show()
    //writeSQLMetadataToKafka(ColumnsTopic, resultOfDBColumns)
    //writeSQLMetadataToKafka("sqlGlobalDataBasesInfo",resultOfGlobalDatabasesInfo)
     writeSQLMetadataToKafka(tablesTopic, resultOfGetSQLDatabasesInfo)
  }*/
}
