package persistance

import org.apache.spark.sql.DataFrame

object DiskWriter {

  def writeToDisk(datatoSave: DataFrame,destinationPath: String): Unit={
    datatoSave.write
      .option(key = "header", value = "true")
      .option(key = "sep", value = ",")
      .option(key = "encoding", value = "UTF-8")
      .option(key = "compresion", value = "none")
      .mode(saveMode = "APPEND")
      .csv("file:///" + destinationPath)
  }

}
