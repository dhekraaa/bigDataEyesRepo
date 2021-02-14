package persistance

import dashboard.Dashboard.{getAllFilesNB, getDBNumber, getDashboardReport, getReportsNumber}
import dataDictionary.DataDictionaryWithCatalogApi.{getAllColumnsInformation, getAllDatabasesInformationWithDesc, getTablesInformationV3, readData}
import dataDictionary.PartitioningDictionary.{OverPartitionnedPercent, countOverPartitionedTablesProb}
import dataProfiling.DataFrameProfile
import dataProfiling.DataProfiling.DataProfiledFormer
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import persistance.KafkaProducer.writeToKafka

class MainSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[*]")
    .getOrCreate()

  "DataPartitioning" should "" in {
    /* Partitioning Report */
    val destinationPath5 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportV2"
    val info = readData(destinationPath5)
    val OVPartProbs: Float = countOverPartitionedTablesProb(info)
    println("OVPartProbs =  " + OVPartProbs)
    val OVProbPercent = OverPartitionnedPercent(OVPartProbs, info)
    println("OVPartPercent =  " + OVProbPercent)

    val percentOV = lit(OVPartProbs).divide(info.count())
    println("partper = " + OVProbPercent)

    val PartKeyProbs = countOverPartitionedTablesProb(info)
    println("key =  " + PartKeyProbs)

    val dfi = info.withColumn("OverPartitionedTablesNb", lit(OVPartProbs.toInt)).withColumn("OVPercent", lit(OVProbPercent)).withColumn("KeyProbNb", lit(PartKeyProbs.toInt))
    dfi.show()
    writeToKafka("DiskDataPartitioning", dfi, "TableName")
    /* /Partitioning Report */

  }
  "DataDictionary " should "" in {
    /*Dictionary*/
    spark.sql("CREATE TABLE boxes (width INT, length INT, height INT) USING CSV COMMENT 'boxes Table '")
    spark.sql("CREATE TABLE sale (employees INT, name String, empsale INT) USING CSV   COMMENT 'Sale Table with importance 1'")
    spark.sql("CREATE  DATABASE  employees with dbproperties('owner'='dhekra')")
    spark.sql("CREATE  DATABASE  employee_db with dbproperties('owner'='dhekra')")
    spark.sql("CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'Customer database' " +
      " WITH DBPROPERTIES (ID=001,'owner'='dhekra')")
    spark.sql("CREATE  TABLE IF NOT EXISTS customer_db.costumers (CustomerName STRING, age INT, hair_color STRING)" +
      "  USING PARQUET OPTIONS(INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'," +
      "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'," +
      "  SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')" +
      "  PARTITIONED BY (hair_color) " +
      " COMMENT 'Table for test'" +
      "  TBLPROPERTIES ('status'='staging', 'owner'='andrew')")
    spark.sql("CREATE TABLE  IF NOT EXISTS employees.employees (EmpName STRING, Id INT, sale STRING)" +
      "  USING PARQUET OPTIONS(INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'," +
      "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'," +
      "  SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')" +
      "  PARTITIONED BY (sale) " +
      " COMMENT 'employees Table' " +
      "  TBLPROPERTIES ('status'='staging', 'owner'='dhekra')")
    spark.sql("CREATE TABLE  IF NOT EXISTS employee_db.employees (EmployeeName STRING, empId STRING, sale STRING)" +
      "  USING PARQUET OPTIONS(INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'," +
      "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'," +
      "  SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')" +
      // "  PARTITIONED BY (sale)" +
      "  TBLPROPERTIES ('status'='staging', 'owner'='andrew')")
    spark.sql("INSERT INTO employees.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO employees.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO employees.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO employee_db.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    /*Databases*/
    val dbInfo: DataFrame = getAllDatabasesInformationWithDesc().withColumn("importance", lit(1))
    dbInfo.show()
    writeToKafka("datadictionary", dbInfo, "databaseName")
    /*Tables*/
    val tinfo: DataFrame = getTablesInformationV3()
    tinfo.show()
   writeToKafka("datadictionaryTables", tinfo, "name")
    //****columns*****/
    val dfcol = getAllColumnsInformation()
    dfcol.show()
   writeToKafka("datadictionaryColumns", dfcol, "colName")

  }
  "dashboard" should "" in {
    /***data*/

    /** dashboard *** */
    val filesDirectory: Array[String] = Array("partitionedsales", "partitionedsalesexp2", "partitionedsalesexp3", "partitionedsalesexp4", "partitionedsalesexp5")
    val reportsDirectories: Array[String] = Array("diskPartitioningReportV2") // "dataFrameProfilingReports", "qualityProfilingReports")
    val dbNB: Integer = getDBNumber()
    val tablesNb: BigInt = getTablesInformationV3().count()
    val filesNB: BigInt = getAllFilesNB("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\", filesDirectory)
    val reportNB = getReportsNumber(reportsDirectories, "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\")
    println("db nb : " + dbNB + " | tables NB : " + tablesNb +
      " | files nb: " + filesNB + " reportNB" + reportNB)
    val seq: Seq[(Integer, BigInt, BigInt, BigInt)] = Seq((dbNB, tablesNb, filesNB, reportNB))
    val dfDashboard: DataFrame = getDashboardReport(seq)
    dfDashboard.show()
    writeToKafka("DashboardTopic", dfDashboard, "DBNB")

    /** * dashboard **************/
  }

  "Data Profiling " should "" in {
    val data2: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv")
    val data2Former = DataProfiledFormer(data2)
    data2Former.show()
    writeToKafka("fileDataProfilerTopic", data2Former.withColumn("SourceName", lit("Sales")), "ColumnName")
    val data: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Costs.csv")
    val data3Former = DataProfiledFormer(data)
     writeToKafka("fileDataProfilerTopic", data3Former.withColumn("SourceName", lit("Costs")), "ColumnName")

  }
  "Data Quality " should "" in {
    /*val data: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv")
       val profiledResult = DataFrameProfile(data).toDataFrame
       val profiledRes: DataFrame = profiledResult.withColumn("SourceName", lit("sales")).withColumn("locationFile", lit("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv"))
       profiledRes.show()
      // writeToKafka("filesDatasProfiler", profiledRes, "SourceName")
       val employeesTab: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Records.csv")
       val profiledResult2 = DataFrameProfile(employeesTab).toDataFrame
       val profRes2 = profiledResult2.withColumn("SourceName", lit("Records")).withColumn("locationFile", lit("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Records.csv"))
     //  writeToKafka("filesDatasProfiler", profRes2, "SourceName")
       val costs: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Costs.csv")
       val profiledResultCosts = DataFrameProfile(costs).toDataFrame
       val profResCosts = profiledResultCosts.withColumn("SourceName", lit("Costs")).withColumn("locationFile", lit("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Costs.csv"))
       //writeToKafka("filesDatasProfiler", profResCosts, "SourceName")
       val dataCust: DataFrame = spark.read.option("header", true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\customers.csv")
       val profiledResultCust = DataFrameProfile(dataCust).toDataFrame
       val profResCust: DataFrame = profiledResultCust.withColumn("SourceName", lit("customers")).withColumn("locationFile", lit("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\customers.csv"))
       //writeToKafka("filesDatasProfiler", profResCust, "SourceName")*/
    val profiledResult3 = DataFrameProfile(baseballDf).toDataFrame
    val profRes3: DataFrame = profiledResult3.withColumn("SourceName", lit("baseball")).withColumn("locationFile", lit("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\baseball")) //.show()
    writeToKafka("filesDatasProfiler", profRes3, "SourceName")
  }

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

  "Data Processing" should "" in {
    val path: String = "file:///C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dataProcessingPartitionsReportV2"
    val df = readData(path).toDF()
    df.show()
    writeToKafka("processingDataPartitioningReport", df, "TotalSize")
  }
  "" should "" in {
    val t = spark.read.format("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\partitioningHistory.csv\\part-00000-d0dde274-f042-4eda-9023-40f55f7cafc8-c000.snappy.parquet")
    println(t.toString())

  }
}
