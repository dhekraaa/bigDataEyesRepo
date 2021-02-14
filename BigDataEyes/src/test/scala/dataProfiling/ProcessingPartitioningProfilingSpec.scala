package dataProfiling

import dataDictionary.ProcessingDataDictionary.getPartitionSize
import dataProfiling.ProcessingPartitioningProfiling._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProcessingPartitioningProfilingSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("DataCatalog")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val nums = List("1", "2", "3", "5846", "78", "1", "78")
  val rdd2: RDD[String] = spark.sparkContext.parallelize(nums)
  val data: DataFrame = nums.toDF()
  val resultOfPartitioningRecommendation: String = "problem in your partitioning strategy, risk of slow processing"
  val resultOfPartitioningRecommendationScenario2: String = "stable"

  "partitioningRecommendation" should "return a partitioning recommendation according to a given seuil using a given partitions size Array" in {
    Given("array of partitions size, seuil ")
    val partitionsSize: Array[Long] = getPartitionSize(data)
    partitionsSize.toSeq.toDF().show()
    val seuil: Int = 2
    When("partitioningRecommendation is invoked")
    val recommendation: String = partitioningRecommendation(partitionsSize, seuil)
    Then("a recommendation should be returned ")
    val expectedResult: String = resultOfPartitioningRecommendation
    println("recommendation: " + recommendation)
    recommendation should contain theSameElementsAs expectedResult
    getAvgRecordsSize(data).toSeq.toDF().show()
  }

  "partitioningRecommendationScenario2" should "return a partitioning recommendation according to a given seuil using a given partitions size Array" in {
    Given("array of partitions size, seuil ")
    val partitionsSize: Array[Long] = getPartitionSize(data)
    partitionsSize.toSeq.toDF().show()
    val seuil2: Int = 7
    When("partitioningRecommendation is invoked")
    val recommendation: String = partitioningRecommendation(partitionsSize, seuil2)
    Then("a recommendation should be returned ")
    val expectedResultSc2: String = resultOfPartitioningRecommendationScenario2
    println("expectedRs= " + expectedResultSc2)
    println("recommendation: " + recommendation)
    recommendation should contain theSameElementsAs resultOfPartitioningRecommendationScenario2
  }

}
