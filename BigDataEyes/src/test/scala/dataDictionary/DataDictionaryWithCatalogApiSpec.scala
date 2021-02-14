package dataDictionary

import dataDictionary.DataDictionaryWithCatalogApi._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import persistance.KafkaProducer._
class DataDictionaryWithCatalogApiSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("DataCatalog")
    .getOrCreate()


  spark.sql("CREATE TABLE boxes (width INT, length INT, height INT) COMMENT 'boxes Table' ")
  spark.sql("CREATE TABLE sale  (employees INT, name String, height INT) COMMENT 'sale Table'")
  spark.sql("CREATE  DATABASE  employees COMMENT 'employees database'  with dbproperties('owner'='dhekra','creationDate'='20/06/2020 08:02 AM')")

  spark.sql("CREATE  DATABASE  employee_db COMMENT 'employee_db database'  with dbproperties('owner'='dhekra','creationDate'='20/06/2020 08:02 AM')")
  spark.sql("CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'Customer database' " +
    " WITH DBPROPERTIES (ID=001, 'creationDate'='20/06/2020 08:02 AM','owner'='dhekra')")
  spark.sql("CREATE  TABLE IF NOT EXISTS my_table (name STRING, age INT, hair_color STRING)" +
    " COMMENT 'my_table Table'" +
    "  " +
    "  PARTITIONED BY (hair_color)" +
    "  TBLPROPERTIES ('status'='staging', 'owner'='andrew')")
  spark.sql("CREATE TABLE  IF NOT EXISTS employees.employees (name STRING, age INT, sale STRING)" +
    " COMMENT 'PARQUET Table'" +
    " " +
    "  PARTITIONED BY (sale)" +
    "  TBLPROPERTIES ('status'='staging', 'owner'='dhekra')")
  spark.sql("CREATE TABLE  IF NOT EXISTS employee_db.employees (name STRING, age INT, sale STRING)" +
    "  COMMENT 'PARQUET Table'" +
    " " +
    "  PARTITIONED BY (sale)" +
    "  TBLPROPERTIES ('status'='staging', 'owner'='dhekra')")
  val expectedResultForReadData: DataFrame = spark.read.option("description", value = true).option("header", value = true).csv("src/test/resources/sales.csv")
  val expectedResultForGetAllDataBasesInformation: DataFrame = spark.catalog.listDatabases().select("name", "description", "locationUri")
  expectedResultForReadData.createTempView("sales")
  val expectedResultForGetTablesInformation: DataFrame = spark.catalog.listTables("default").select("name", "database", "description", "isTemporary", "tableType")
  val expectedResultForGetColumnsInformation: DataFrame = spark.catalog.listColumns("sales").toDF()

  "readData" should "read all data from a csv file" in {
    Given("csv file path, implicit Spark session")
    //todo: the resources package must be in test package
    val CSVFilePath = "src/test/resources/sales.csv"
    When("readData is invoked")
    val dataRead = readData(CSVFilePath)
    Then("all data from the given path should be returned as a dataFrame")
    val expectedResultDF = expectedResultForReadData
    dataRead.collect() should contain theSameElementsAs expectedResultDF.collect()
  }

  "getAllDataBasesInformation" should "return all databases information" in {
    Given("an implicit Spark session")
    When("getAllDataBasesInformation is invoked")
    val allDataBasesInformation = getAllDataBasesInformation()
    Then("all databases information should be returned")
    val expectedResultForGetAllDataBasesInformationDF = expectedResultForGetAllDataBasesInformation.toDF()
    allDataBasesInformation.collect() should contain theSameElementsAs expectedResultForGetAllDataBasesInformationDF.collect()
  }

  "getTablesInformation" should "return all Tables information of a given database " in {
    Given("database name, an implicit Spark session")
    val databaseName = "default"
    When("getTablesInformation is invoked")
    val tablesInformation = getTablesInformation(databaseName)
    Then("all tables information of default database should be returned ")
    val expectedResultForGetTablesInformationDF: DataFrame = expectedResultForGetTablesInformation
    tablesInformation.collect() should contain theSameElementsAs expectedResultForGetTablesInformationDF.collect()
  }

  "getAllDatabasesInformationWithDesc" should "" in {
    val dbInfo: DataFrame = getAllDatabasesInformationWithDesc()
    writeToKafka("datadictionary",dbInfo,"databaseName")
  }
  "" should "" in {
    val tablesInfo: DataFrame = getTablesInformationV2()
    writeToKafka("datadictionaryTables",tablesInfo,"name")

  }

  "getColumnsInformation" should "return all columns information of a given table" in {
    Given(" table name, implicit Spark session")
    val tableName = "sales"
    When("getColumnsInformation is invoked")
    val columnsInformation = getColumnsInformation(tableName)
    Then("all columns information of Sales Table should be returned")
    val expectedResultGetColumnsInformationDF = expectedResultForGetColumnsInformation
    columnsInformation.collect() should contain theSameElementsAs expectedResultGetColumnsInformationDF.collect()
  }

}
