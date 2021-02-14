package dataDictionary

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataDictionaryWithCatalogApi {
  var dataFrame: DataFrame = _

  //todo : data should be read with format and not csv
  def readData(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.option("description", value = true).option("header", value = true).option("nullable", value = true).option("isPartition", value = true).option("isBucket", value = true).option("dataType", value = true).csv(path)
  }

  //todo : parametrize the owner of databases to show
  def getAllDataBasesInformation()(implicit spark: SparkSession): DataFrame = {
    spark.catalog.listDatabases().select("name", "description", "locationUri")
  }

  def getDatabaseInformation(dataDescCmd: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    var res: Seq[(String, String, String, String)] = Seq()
    val result: List[Seq[(String, String, String, String)]] = List() //res.toDF("ColN","count","mean","stddev","min","max")

    val Cols: Array[String] = dataDescCmd.columns
    val dataProfiled: DataFrame = dataDescCmd
    //for (i <- 0 to Cols.size - 1) {
    var line = dataProfiled.select(Cols(1)).collectAsList()
    var s: Seq[(String, String, String, String)] = Seq((line.get(0).getString(0), line.get(1).getString(0), line.get(2).getString(0), line.get(3).getString(0)))
    s.toDF(dataProfiled.select(Cols(0)).collectAsList().get(0).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(1).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(2).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(3).getString(0))
    res ++= s

    // }
    res.toDF(dataProfiled.select(Cols(0)).collectAsList().get(0).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(1).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(2).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(3).getString(0))
    //res
  }

  def getAllDatabasesInformationWithDesc()(implicit spark: SparkSession): DataFrame = { ///this new Databases Description
    import spark.implicits._

    var res: Seq[(String, String, String, String, Long)] = Seq()
    val result: List[Seq[(String, String, String, String)]] = List() //res.toDF("ColN","count","mean","stddev","min","max")
    val DatabasesName = getAllDataBasesInformation().select("name").collectAsList()
    for (i <- 0 to DatabasesName.size() - 1) {

      var df: DataFrame = spark.sql("DESCRIBE DATABASE EXTENDED " + (DatabasesName.get(i)).getString(0).replaceAll("[=\\s].*", ""))
      val Cols: Array[String] = df.columns
      val dataProfiled: DataFrame = df
      //for (i <- 0 to Cols.size - 1) {
      var line = dataProfiled.select(Cols(1)).collectAsList()
      var s: Seq[(String, String, String, String, Long)] = Seq((line.get(0).getString(0), line.get(1).getString(0), line.get(2).getString(0), line.get(3).getString(0).replaceAll("(=\\s).*", ""), spark.catalog.listTables((DatabasesName.get(i)).getString(0).replaceAll("[=\\s].*", "")).count()))
      s.toDF(dataProfiled.select(Cols(0)).collectAsList().get(0).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(1).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(2).getString(0), dataProfiled.select(Cols(0)).collectAsList().get(3).getString(0), "count")
      res ++= s
    }

    res.toDF("databaseName", "description", "location", "Properties", "TablesNb")
    //res
  }

  def getTablesInformation(DataBaseName: String)(implicit spark: SparkSession): DataFrame = {
    spark.catalog.listTables(DataBaseName).select("name", "database", "description", "tableType", "isTemporary", "tableType")
  }

  def getTablesInformation()(implicit spark: SparkSession): DataFrame = {
    spark.catalog.listTables().select("name", "database", "description", "tableType", "isTemporary", "tableType")
  }

  def getTablesInformationV2()(implicit spark: SparkSession): DataFrame = {

    val databases = spark.catalog.listDatabases().select("name").collectAsList()
    var tabDf = spark.catalog.listTables(databases.get(0).getString(0))
    for (i <- 1 to databases.size() - 1) {
      tabDf = tabDf.union(spark.catalog.listTables(databases.get(i).getString(0)))
    }
    tabDf.toDF("name", "database", "description", "tableType", "isTemporary")
  }

  def getTablesInformationV3()(implicit spark: SparkSession): DataFrame = {
    val dbs = spark.catalog.listDatabases()
    val dbnames: Array[String] = Array("employee_db", "default", "employees", "customer_db")
    val countcol0 = spark.catalog.listColumns(dbnames(0), "employees").count()
    println("nb of columns0 " + countcol0)
    val countLine0 = spark.sql("SELECT COUNT(*) as count FROM " + dbnames(0) + "." + "employees").select("count").collectAsList().get(0).toSeq.mkString.replaceAll("[=\\s].*", "")
    val importance: Long = 1
    var s0: DataFrame = spark.catalog.listTables(dbnames(0)).withColumn("rowCount", lit(countLine0)).withColumn("columnCount", lit(countcol0)).withColumn("importance", lit(importance))
    var l = s0
    for (j <- 1 to dbnames.length - 1) {
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
        l = s.withColumn("rowCount", lit(countLine)).withColumn("columnCount", lit(countcol)).withColumn("importance", lit(importance)) //.show()
        s0 = s0.distinct().union(l)
        var res: DataFrame = s0.distinct().union(l)
        //l.show()
        //res.show()
      }
      l = s0.distinct().union(l)
    }
    //l.distinct().show()
    l.dropDuplicates()
  }

  //todo: Add database name to parameter
  def getColumnsInformation(TableName: String)(implicit spark: SparkSession): DataFrame = {
    spark.catalog.listColumns(TableName).toDF()
  }


  def getAllColumnsInformation()(implicit spark: SparkSession): DataFrame = {

    val dbnames: Array[String] = Array("employee_db", "default", "employees", "customer_db")
    val tablesOfDb0 = spark.catalog.listTables(dbnames(0)).select("name").collectAsList()
    val importance: Long = 1
    var dfres: DataFrame = spark.catalog.listColumns(dbnames(0), tablesOfDb0.get(0).getString(0))
      .withColumn("databaseName", lit(dbnames(0)))
      .withColumn("TableName", lit(tablesOfDb0.get(0).getString(0)))
      //.withColumn("occurrence", approx_count_distinct(spark.catalog.listColumns(dbnames(0), tablesOfDb0.get(0).getString(0)))))
      .withColumn("importance", lit(importance))
    for (i <- 0 to dbnames.length - 1) {
      var dbname = dbnames(i)
      val tablesOfThisDb = spark.catalog.listTables(dbname).select("name").collectAsList()
      for (j <- 0 to tablesOfThisDb.size() - 1) {
        var f = spark.catalog.listColumns(dbnames(i), tablesOfThisDb.get(j).getString(0))
          .withColumn("databaseName", lit(dbnames(i)))
          .withColumn("TableName", lit(tablesOfThisDb.get(j).getString(0)))
          .withColumn("importance", lit(importance))
        dfres = dfres.union(f)
      }

    }
    dfres.toDF("colName","Coldescription","dataType","nullable","isPartition","isBucket","ColdatabaseName","ColTableName","Colimportance").dropDuplicates()
  }

}
