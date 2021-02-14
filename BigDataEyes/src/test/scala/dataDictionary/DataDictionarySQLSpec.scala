package dataDictionary

import java.io.File

import dataDictionary.DataDictionarySQL._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import persistance.KafkaProducer.writeToKafka
class DataDictionarySQLSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  implicit val spark: SparkSession = SparkSession.builder
    .config("spark.sql.warehouse.dir",warehouseLocation)
    .enableHiveSupport()
    .master("local[*]")
    .appName("BigDataEyes")
    .getOrCreate()

  implicit val jdbcUrlMySql = "jdbc:mysql://localhost:3306/datacatalog"
  val jdbcHostname = "localhost"
  val jdbcPort =  1433
  val jdbcDatabase = "Sales_DW"
  val jdbcUsernameMYSQL: String = "root"
  val jdbcPasswordMYSQL: String = ""
  val jdbcUsernameSqlServer: String = "dhekraUser"
  val jdbcPasswordSqlServer: String = "dhekraUser"
  implicit val jdbcUrlSqlServer = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};instanceName=SQLEXPRESS;database=${jdbcDatabase}"

  "getGlobalDatabasesInformations" should "return global databases informations" in  {
    Given("database URL, User Name and Password")
    When("getGlobalDatabasesInformations is invoked")
    val globalDataBasesInfoMySQL = getGlobalDatabasesInformations(jdbcUrl = jdbcUrlMySql, jdbcUsernameMYSQL,jdbcPasswordMYSQL)
    val globalDataBasesInfoSQLServer = getGlobalDatabasesInformations(jdbcUrl = jdbcUrlSqlServer,jdbcUsernameSqlServer,jdbcPasswordSqlServer)
    Then("all data from the given path should be returned as a dataFrame")
    globalDataBasesInfoMySQL.show()
    globalDataBasesInfoSQLServer .show()
  }
  "getDatabasesTablesDetails_SQL" should "return all databases informations" in  {
    Given("database URL, User Name and Password")
    When("getGlobalDatabasesInformations is invoked")
    val allDataBasesInfoMySQL = getDatabasesTablesDetails_SQL(jdbcUrl = jdbcUrlMySql, jdbcUsernameMYSQL,jdbcPasswordMYSQL)
    val allDataBasesInfoSQLServer = getDatabasesTablesDetails_SQL(jdbcUrl = jdbcUrlSqlServer,jdbcUsernameSqlServer,jdbcPasswordSqlServer)
    Then("all data from the given path should be returned as a dataFrame")
    allDataBasesInfoMySQL.show()
    allDataBasesInfoSQLServer.show()
  }
  "getDataBaseColumnsDetails" should "return all databases informations" in  {
    Given("database URL, User Name and Password")
    When("getDataBaseColumnsDetails is invoked")
    val allDataBasesInfoMySQL = getDataBaseColumnsDetails(jdbcUrl = jdbcUrlMySql, jdbcUsernameMYSQL,jdbcPasswordMYSQL)
    // val globalDataBasesInfoSQLServer = getGlobalDatabasesInformations(jdbcUrl = jdbcUrlSqlServer,jdbcUsernameSqlServer,jdbcPasswordSqlServer)
    Then("all data from the given path should be returned as a dataFrame")
    allDataBasesInfoMySQL.show()
   // allDataBasesInfoMySQL.write.format("hive").saveAsTable("testingHive")
    // globalDataBasesInfoSQLServer .show()
  }
  "" should "" in {
    import spark.implicits._
    def baseballDf: DataFrame = {
      Seq(
        ("Mets", "1986", "New York", "nick"),
        ("Yankees", "2009", "New York", "dave"),
        ("Cubs", "2016", "Chicago", "bruce"),
        ("White Sox", "2005", "Chicago", null),
        ("Nationals", "", "Washington", null)
      ).toDF("team", "lastChampionship", "city", "fan")
    }
    baseballDf.write.format("hive").saveAsTable("testingHive_2")
    spark.sql("select * from testingHive_2").show()
    val TableMetastore : DataFrame = spark.sql("DESCRIBE EXTENDED testingHive_2")
    TableMetastore.show()
    writeToKafka("HiveTableMetadata2",TableMetastore,"col_name")
    //TableMetastore.columns.toSeq.toDF().show()
  }
}


