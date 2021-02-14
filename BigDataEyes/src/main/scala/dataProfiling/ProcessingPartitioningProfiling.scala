package dataProfiling

import dataDictionary.ProcessingDataDictionary.{getNumberOfRecordsPerPartition, getRDDOfDataFrame, getRecordSize}
import org.apache.spark.sql.DataFrame

object ProcessingPartitioningProfiling {
  def getMaxPartitionSize(data: Array[Long]): Long = {
    data.foldLeft((data(0))) { case ((max), e) => (math.max(max, e)) }
  }

  def getMinPartitionSize(data: Array[Long]): Long = {
    data.foldLeft((data(0))) { case ((min), e) => (math.min(min, e)) }
  }

  def getAvgPartitionSize(data: Array[Long], partitionsNumber: Long): Long = {
    var i = 0
    var sum = 0
    while ( {
      i < data.length
    }) {
      sum += data(i).toInt
      i += 1
    }
    sum / partitionsNumber.toInt
  }

  def getAvgRecordsSize(data: DataFrame): Array[Long] = {
    var i = 0
    var sum = 0
    var avg: Long = 0
    var result: Array[Long] = Array()
    val recordsNumberPerPartition = getNumberOfRecordsPerPartition(data)
    val dataRDD = getRDDOfDataFrame(data)
    val recordsSize = getRecordSize(dataRDD) //(0)
    val recordsArraysize = recordsNumberPerPartition.size - 1
    for (i <- 0 to recordsArraysize) {
      var j = i
      var sum: Long = 0
      while (j < recordsNumberPerPartition(i)) {
        sum += recordsSize(j)
        j += 1
      }
      avg = sum / recordsNumberPerPartition(i)
      println(avg)
      result :+= avg
    }
    result
  }

  def getPartitioningState(partitionNumber: Long, coresNumber: Int): String = {
    var state: String = ""
    if (coresNumber < partitionNumber ) {
      state = "Slow Processing Speed"
    } else if (coresNumber > partitionNumber ) {
      state = "Wasteful Resources"
    }
    else if(coresNumber == partitionNumber ) {
      state = "Normal"
    }
    state
  }

  def getPartitoinsStateRecommendadtion(partitionNumber: Long, coresNumber: Int): String = {
    var state: String = ""
    if (coresNumber < partitionNumber) {
      state = "Having too many threads per container might increase contention and slow you down."
    } else if (coresNumber > partitionNumber ) {
      state = "You need to have more partitions (= tasks in a given stage) than you have cores, otherwise some cores will be sitting around doing nothing."
    }
    state

  }

  def getPartitionsSizeRecommendation(AvgPartSize: Long, partSizeSeuil: Long): String = {
    var res: String = ""
    if (AvgPartSize < partSizeSeuil)
      res = "Small partitions lead to slower jobs because there's a certain amount of communication between " +
              "the driver and the slaves, and it does amount to a lot of time"
    else
      res = "It would be more efficient to break that up into smaller partitions so other Executors and Tasks " +
              "can process it in parallel as opposed to waiting for the big partition to processed by one Task"
    res
  }

  def partitioningRecommendation(partionsSize: Array[Long], seuil: Long): String = {
    var result: String = ""
    var i = 0
    var diff: Long = 0
    val len = partionsSize.length - 1
    println("seuil = " + seuil)
    while ( {
      (i < len && diff < seuil)
    }) {
      println("len= " + len)
      diff = partionsSize(i + 1) - partionsSize(i)
      i += 1
      println(diff)
    }
    println("i = " + i)
    println("len = " + len)
    if (diff < seuil)
      result = "stable"
    else
      result = "Use at least as many cores as partitions, By increasing the number of containers, " +
                "You can make the job 4 times faster."
    result
  }

}
