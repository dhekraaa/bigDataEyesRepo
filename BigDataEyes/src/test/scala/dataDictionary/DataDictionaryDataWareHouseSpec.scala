package dataDictionary

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataDictionaryDataWareHouseSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[2]")
    .getOrCreate()
"" should "" in {
 // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

  val jdbcHostname = "localhost"
  val jdbcPort =  1433
  val jdbcDatabase = "Sales_DW"

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};instanceName=SQLEXPRESS;database=${jdbcDatabase}"
  import java.util.Properties
  val connectionProperties = new Properties()
  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   connectionProperties.setProperty("Driver", driverClass)
  val c = spark.sqlContext
    .read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("user","dhekra")
    .option("password","kassaoui")
    .option("driver", driverClass)
    .option("query","SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
    .load()
  c.show()
  //jdbc(jdbcUrl,"DimCustomer",connectionProperties)
  // Create a Properties() object to hold the parameters.
 //
//  val connectionProperties = new Properties()
//  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
 // connectionProperties.setProperty("Driver", driverClass)
  //val connectionProperties = new Properties()
 //val employees_table = spark.read.jdbc(jdbcUrl,"DimCustomer",connectionProperties) // .jdbc(jdbcUrl, "employees")
}

}
