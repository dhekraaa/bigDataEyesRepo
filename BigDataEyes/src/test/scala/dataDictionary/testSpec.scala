package dataDictionary

import dataDictionary.DataDictionaryWithCatalogApi._
import dataDictionary.PartitioningDictionary._
import dashboard.Dashboard._
import dataProfiling.DataProfiling._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.row_number
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import persistance.KafkaProducer.writeToKafka

class testSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[2]")
    .getOrCreate()
  "" should "" in {
    import spark.implicits._
    val destinationPath1 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReport"
    //    val partReport1 = readData(destinationPath1)
    import org.apache.spark.sql.functions.{sum, when, count}
    // val partProbNB = partReport1.agg(count($"PartitionRecommendation")).toDF("partProbNB")
    // println("PartitionRecommendation if not null :"+partReport1.agg(sum(when($"PartitionRecommendation".isNotNull,0).otherwise(1))/count("*")))
    // println("PartitionRecommendation count: "+partReport1.select("PartitionRecommendation").count())
    import org.apache.spark.sql.functions._
    //partReport1.withColumn("partProbNB", partReport1.agg(count($"PartitionRecommendation")).toDF("partProbNB").col("partProbNB")).show()
    //val df = partReport1.join(partProbNB,"partProbNB")
    //df.show()
    // partProbNB.show()
    println("djdgvjnjjjjùllbubhuybbuubub \n njnmkj,jjksksck")
    val struct: StructType =
      StructType(
        StructField("transactionId", StringType, nullable = true) ::
          StructField("customerId", StringType, nullable = false) ::
          StructField("itemId", StringType, nullable = false) ::
          StructField("amountPaid", StringType, nullable = false) :: Nil)
    val data: DataFrame = spark.read.option("header", true).csv("src/test/resources/sales.csv")
    // spark.catalog.createTable("costumers","C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\db/",struct)
    //  spark.catalog.createTable("sales","C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\db/","C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv")//.createTable("sales","C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv")
    // spark.catalog.createTable("employees","C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\db/",struct)
    spark.sql("CREATE TABLE boxes (width INT, length INT, height INT) USING CSV COMMENT 'boxes Table '")
    spark.sql("CREATE TABLE sale (employees INT, name String, empsale INT) USING CSV   COMMENT 'Sale Table with importance 1'")
    spark.sql("CREATE  DATABASE  employees with dbproperties('owner'='dhekra')")
    //spark.sql("CREATE TABLE employees.EmployeeBoxes (width INT, length INT, height INT) USING CSV COMMENT 'boxes Table '")
    //spark.sql("CREATE TABLE employees.EmployeeSales (employees INT, name String, height INT) USING CSV   COMMENT 'employees.Sale Table with importance 1'")
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
    spark.sql("CREATE TABLE  IF NOT EXISTS employee_db.employees (name STRING, empId STRING, sale STRING)" +
      "  USING PARQUET OPTIONS(INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'," +
      "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'," +
      "  SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')" +
      // "  PARTITIONED BY (sale)" +
      "  TBLPROPERTIES ('status'='staging', 'owner'='andrew')")
    /* spark.sql("CREATE TABLE  IF NOT EXISTS employees.EmployeeSales (employees INT, name String, height INT)" +
       "  USING PARQUET OPTIONS(INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'," +
       "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'," +
       "  SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')" +
       " COMMENT 'employees.Sale Table with importance 1'" +
       // "  PARTITIONED BY (sale)" +
       "  TBLPROPERTIES ('status'='staging', 'owner'='andrew')")*/
    //getTablesInformation().show()
    //println("nb of tables: " + getTablesNumber())
    // spark.sql("DESCRIBE DATABASE EXTENDED default").show()
    // spark.sql("ALTER DATABASE sa set ownerName dhekra")
    //val df = spark.sql("DESCRIBE DATABASE EXTENDED sa")
    // val df2 = spark.sql("DESCRIBE DATABASE EXTENDED customer_db") //.show()
    //getAllDataBasesInformation().show()
    //spark.catalog.listDatabases().show()

    // spark.sql("SHOW DATABASES").show()
    getAllDatabasesInformationWithDesc().show()
    //val Cols: Array[String] = df.columns
    //  df.select(Cols(1)).collectAsList().get(1).getString(0)
    //println("desc= en get(2) "+df.select(Cols(1)).collectAsList().get(2).getString(0))
    //getDatabaseInformation(df).show()
    // getDatabaseInformation(df2).show()
    //getAllDatabasesInformationWithDESCRIBE().show()
    //println("desc en get(1)=  "+df.select(Cols(1)).collectAsList().get(1).getString(0))
    // println("desc= get(3):  "+df.select(Cols(1)).collectAsList().get(3).getString(0))
    //spark.sql("DESCRIBE DATABASE EXTENDED sa").show()
    // println("cols : "+Cols(0).toSeq.mkString(", "))
    //println("desc=  "+df.select(Cols(1)).collectAsList().get(5).getString(0))

    println("tablesnb = " + getTablesNumber())

    // val d = dataProfiler(data)
    import org.apache.spark.sql.functions._
    val df = spark.read.option("header", "true").csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv") //.createTempView("sales")
    //spark.catalog.listColumns("employee_db","employees").withColumn("c",row_number()).show()

    println("nb of lines table sales : " + df.count())
    //val df2 = spark.sqlContext.read.parquet("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\employee_db.db\\employees")
    println("nb of lines table employees : ")
    spark.sql("INSERT INTO employees.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO employees.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO employees.employees VALUES ('dhekra',25,2800) ")
    spark.sql("INSERT INTO employee_db.employees VALUES ('dhekra',25,2800) ")
    // spark.sql("INSERT INTO employees.EmployeeSales VALUES (1,'dhekra',2800) ")
    //spark.sql("INSERT INTO employees.EmployeeSales VALUES (1,'Nejma',2800) ")
    // spark.sql("INSERT INTO employees.EmployeeBoxes VALUES (1,5,280) ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")
    spark.sql("INSERT INTO customer_db.costumers VALUES ('kas',5,'280') ")


    spark.sql("SELECT COUNT(*) FROM employees.employees")
    //println("nb of LINES = "+spark.sql("SELECT COUNT(*) as count FROM employees.employees").select("count").collectAsList().get(0).toSeq.mkString.replaceAll("[=\\s].*", "")) //.show() //celci
    val df3 = spark.catalog.listTables("default")
    val df6 = spark.catalog.listTables("employee_db")
    df3.union(df6).show()
    getTablesInformationV2().show()
    // spark.catalog.listTables("default").withColumn("c",row_number()).show()
    println("default DB tables count = " + spark.catalog.listTables("default").count())
    println("default DB tables count = " + spark.catalog.listTables("employee_db").count())
    println("nb of columns " + spark.catalog.listColumns("employee_db", "employees").count()) //celci
    //spark.catalog.listTables("employee_db").withColumn("count",spark.sql("SELECT COUNT(*) FROM employee_db.employees").col("count(1)"))
    //d.show()t
    println("tab name : " + getTablesInformation().select("name").collectAsList().get(0).getString(0))
    println("database : " + getTablesInformation().select("database").collectAsList().get(1).getString(0))
    getTablesInformation().show()
    val d = spark.catalog.listDatabases()
    spark.catalog.listDatabases().show()
    spark.catalog.listTables("employee_db").show()
    spark.catalog.listTables(d.select("name").collectAsList().get(1).getString(0)).show()
    spark.catalog.listTables(d.select("name").collectAsList().get(0).getString(0)).show()
    spark.catalog.listTables(d.select("name").collectAsList().get(2).getString(0)).show()
    // val d8 = getTablesInformationV2().select("database")
    /*spark.catalog.listColumns(d8.select("database").collectAsList().get(0).getString(0), d8.select("name").collectAsList().get(0).getString(0)).show()
    val f = spark.catalog.listColumns(d8.select("database").collectAsList().get(0).getString(0), d8.select("name").collectAsList().get(0).getString(0))
    import org.apache.spark.sql.functions.lit
    var s = f.withColumn("databaseN", lit(d8.select("database").collectAsList().get(0).getString(0)))
    f.show()*/
    //lit
    //var l = s.withColumn("tabbN", lit(d8.select("name").collectAsList().get(0).getString(0)))
    //var resi
    /*for (i <- 1 to d8.select("database").collectAsList().size() - 1) {
      var f = spark.catalog.listColumns(d8.select("database").collectAsList().get(i).getString(0), d8.select("name").collectAsList().get(0).getString(0))
      var r = f.withColumn("databaseN", lit(d8.select("database").collectAsList().get(i).getString(0)))
      f.show()
      //var ts = r.withColumn("tabbN", lit(d8.select("name").collectAsList().get(i).getString(0)))
      s = s.union(r)
      //.add(d8.col("database"))
      //getColumnsInformation().show()
    }
    s.toDF("name", "description", "dataType", "nullable", "isPartition", "isBucket", "databaseN", "tabbN2").show()
*/
    // getTablesInformationV2().show()
    val dbInfo: DataFrame = getAllDatabasesInformationWithDesc().withColumn("importance",lit(1))
     writeToKafka("datadictionary",dbInfo,"databaseName")
    val tablesInfo: DataFrame = getTablesInformationV2()
    //  writeToKafka("datadictionaryTables",tablesInfo,"name")
    //getColumnsInformation(TableName: String).show()
    /* val dbs = spark.catalog.listDatabases()
     val dbnames: Array[String] = Array("employee_db", "default","employees")
     val countcol0 = spark.catalog.listColumns(dbnames(0), "employees").count()
     println("nb of columns0 " + countcol0)
     val countLine0 = spark.sql("SELECT COUNT(*) as count FROM " + dbnames(0) + "." + "employees").select("count").collectAsList().get(0).toSeq.mkString.replaceAll("[=\\s].*", "")
     val importance = 1
     var s0: DataFrame = spark.catalog.listTables(dbnames(0)).withColumn("countl", lit(countLine0)).withColumn("countC", lit(countcol0)).withColumn("importance",lit(importance))
     var l = s0
     for (j <- 0 to dbnames.length - 1) {
       val s = spark.catalog.listTables(dbnames(j))
       var dbname = dbs.select("name").collectAsList().get(j).getString(0)
       var namedb = dbnames(j)
       var countcol: Long = 0
       var countLine: String = ""
       for (i <- 0 to s.select("name").collectAsList().size() - 1) {
         countcol = spark.catalog.listColumns(namedb, s.select("name").collectAsList().get(i).getString(0)).count()
         println("nb of columns " + countcol)
         countLine = spark.sql("SELECT COUNT(*) as count FROM " + namedb + "." + s.select("name").collectAsList().get(i).getString(0)).select("count").collectAsList().get(0).toSeq.mkString.replaceAll("[=\\s].*", "")
         println("nb of LINES = " + countLine)
         l = s.withColumn("countl", lit(countLine)).withColumn("countC", lit(countcol)).withColumn("importance",lit(importance))//.show()
         s0 = s0.union(l)
         var res: DataFrame = s0.union(l)
         l.show()
         res.show()
       }
       l = s0.union(l)
       //s.withColumn("countl", lit(countLine)).withColumn("countC", lit(countcol)).show()
     }
     //l = s0.union(l)
     //s0 = s0.union(l)

     //s0.show()
     l.distinct().show()
 */
    spark.catalog.listTables("employees").select("name", "database", "description", "tableType", "isTemporary").show()
    val tinfo: DataFrame = getTablesInformationV3()
    //tinfo.distinct().show()
    // tinfo.select("name").dropDuplicates().show()
    tinfo.show()
    writeToKafka("datadictionaryTables",tinfo,"name")


    //****columns*****/
    val dfcol = getAllColumnsInformation()
    dfcol.show()
   writeToKafka("datadictionaryColumns", dfcol, "colName")
    val dash = spark.read.option("header", value = true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport\\part-00000-6dc192ee-e628-4464-a3ea-2a497e73cad5-c000.csv")

    writeToKafka("DashboardTopic",dash,"DBNB")
    /** dashboard *** */
      val filesDirectory: Array[String] = Array("partitionedsales", "partitionedsalesexp2", "partitionedsalesexp3", "partitionedsalesexp4", "partitionedsalesexp5")
      val reportsDirectories: Array[String] = Array("diskPartitioningReportV2") //, "dataFrameProfilingReports", "qualityProfilingReports")
      val dbNB: Integer = getDBNumber()
      val tablesNb: BigInt =  getTablesInformationV3().count()
      val filesNB: BigInt = getAllFilesNB("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\spark-warehouse\\", filesDirectory)
      val reportNB = getReportsNumber(reportsDirectories, "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\")
      println("db nb : " + dbNB + " | tables NB : " + tablesNb +
        " | files nb: " + filesNB + " reportNB" + reportNB)
      val seq: Seq[(Integer, BigInt, BigInt, BigInt)] = Seq((dbNB, tablesNb, filesNB, reportNB))
      val dfDashboard: DataFrame = getDashboardReport(seq)
      dfDashboard.show()
      // writeToDisk(dfDashboard,"C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport")
      //val d = spark.read.option("header", value = true).csv("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport\\part-00000-6dc192ee-e628-4464-a3ea-2a497e73cad5-c000.csv")

      //readData("C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\dashboardReport")
      writeToKafka("DashboardTopic", dfDashboard, "DBNB")
    /*** dashboard **************/
  }
  //  println("description of a specific tables: "+spark.catalog.getTable("boxes").description)
  // println("description of a specific tables: "+spark.catalog.getTable("boxes").description)

}

/*
val Cols: Array[String] = data.columns
println(Cols.toSeq.mkString(", "))
data.select(Cols(0)).count()
d.select(Cols(0)).show()
val line1 = d.select(Cols(0)).collectAsList()
val s :Seq[(String,String,String,String,String,String)] = Seq((Cols(0), line1.get(0).getString(0),line1.get(1).getString(0),line1.get(2).getString(0),line1.get(3).getString(0), line1.get(4).getString(0)))
s.toDF("ColN","count","mean","stddev","min","max").show()
/****/
// val d2 = DataProfiledFormer(data)
// d2.distinct().show()
//println(d2.groupBy("SrcN"))

//val c = data.withColumn("input_file", input_file_name())
//val col: Column = c.col("input_file")
//  d2.show()
// val Cols: Array[String] = data.columns
//println(Cols.toSeq.mkString(", "))
//data.describe(data.columns :_* ).show()
//val row = data.agg(maxCols.head, maxCols.tail: _*).head()
//data.describe(Cols.toSeq.toDF().collectAsList().get(0).getString(0),Cols.toSeq.toDF().collectAsList().get(1).getString(0),"itemId","amountPaid").show()
/*data.describe("transactionId","customerId","itemId","amountPaid").show()
val maxCols: Array[Column] = data.columns.map(max)
val row = data.agg(maxCols.head, maxCols.tail: _*).head() //les noms des columns
//data.agg(maxCols.)
println(maxCols.toSeq.mkString(", "))
*/
//spark.catalog.listTables().show()
//partReport1.show()
/*val destinationPath2 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportExp2"
val partReport2 = readData(destinationPath2)
partReport2.show()
val destinationPath3 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportExp3"
val partReport3 = readData(destinationPath3)
partReport3.show()
val destinationPath4 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportExp4"
val partReport4 = readData(destinationPath4)
partReport4.show()
val destinationPath5 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\diskPartitioningReportExp5"
val partReport5 = readData(destinationPath5)
partReport5.show()*/
}
}*/
