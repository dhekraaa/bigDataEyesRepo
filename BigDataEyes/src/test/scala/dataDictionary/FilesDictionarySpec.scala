package dataDictionary
import dataDictionary.FilesDictionary._
import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import persistance.KafkaProducer._

class FilesDictionarySxpec extends AnyFlatSpec with Matchers with GivenWhenThen{

  "" should "" in {
    val filePath = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\sales.csv"
    val res: DataFrame = getFileProprety(filePath)
    writeToKafka("fileDictionary", res, "FileName")

    val filePath2 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Costs.csv"
    val res2: DataFrame = getFileProprety(filePath2)
    writeToKafka("fileDictionary", res2, "FileName")

    val filePath3 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\customers.csv"
    val res3: DataFrame = getFileProprety(filePath3)
    writeToKafka("fileDictionary", res3, "FileName")

    val filePath4 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\Records.csv"
    val res4: DataFrame = getFileProprety(filePath4)
    writeToKafka("fileDictionary", res4, "FileName")

    val filePath5 = "C:\\Users\\kassa\\OneDrive\\Bureau\\DataCatalogProjectWithTDDMethodology\\src\\test\\resources\\News.parquet"
    val res5: DataFrame = getFileProprety(filePath5)
    writeToKafka("fileDictionary", res5, "FileName")

  }


}
