package hotspot

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import preprocess.DatasetStatistics

import scala.collection.concurrent.TrieMap

object Analyzer {

  def calculateGetisOrdStatistics(spark: SparkSession,  itemsCounts: TrieMap[String, Int], numOfItems: Long) = {
    val sumAccum = spark.sparkContext.doubleAccumulator("sumAccum")
    val squareSumAccum = spark.sparkContext.doubleAccumulator("squareSumAccum")

    itemsCounts.foreach(item=>{
      sumAccum.add(item._2)
      squareSumAccum.add(item._2*item._2)
    })

    new GetisOrdStatistics(sumAccum.value, squareSumAccum.value, numOfItems)
  }

  def calculateGetisOrd(spark: SparkSession,  itemsCounts: TrieMap[String, Int], datasetStatistics: DatasetStatistics) = {
    /*
    var result: List[Tuple2[String, Int]]

    itemsCounts.flatMap(item=>{

      Array(item._2, item._1)
    })

    */

  }


  def calculateStatistics(semanticAreasCounts: TrieMap[String, Int], bcDatasetStatistics: Broadcast[DatasetStatistics])= {
    val distanceInCells = bcDatasetStatistics.value.params.getPropertyValue("neighborDistanceInCells").toInt
    val weightFactor = bcDatasetStatistics.value.params.getPropertyValue("weightFactor").toInt
    var result: List[(Long, Double)] = List()
    val t = semanticAreasCounts.flatMap(areaKV=>{
      for (h <- 0 to distanceInCells){
        if (h==0) result.::(areaKV._1, weightFactor)
        else{
          var maxDistance: Int = 0
          areaKV._1.split(",").foreach(areaDim=>{
            //var maxCell = bcDatasetStatistics.value.
            if ((areaDim.toInt - h >= 0) && (areaDim.toInt + h < 5)){

            }
          })
        }
      }
      result.iterator
    })
  }





}
