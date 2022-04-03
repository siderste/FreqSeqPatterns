package preprocess

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DataPreprocessing {

  def getStringDimensions(params: Parameters): Array[String]={
    var stringDims: ArrayBuffer[String] = ArrayBuffer()
    val dimsTypes = params.getPropertyValue("dimensionTypes").split(",")
    for (dim <- 0 to dimsTypes.length-1){
      dimsTypes(dim) match {
        case "L" =>
        case "D" =>
        case "S" => stringDims += "d_"+dim
      }
    }
    stringDims.toArray
  }

  def readInput(spark: SparkSession, params: Parameters) : DataFrame = {
    var schema = new StructType()
    val dimsTypes = params.getPropertyValue("dimensionTypes").split(",")
    val validDims = params.getPropertyValue("selectedDimensions").split(",")
    var allColumnsNames: ArrayBuffer[String] = ArrayBuffer()
    var validColumnsNames: ArrayBuffer[String] = ArrayBuffer()
    for (dim <- 0 to dimsTypes.length-1){
      dimsTypes(dim) match {
        case "L" => {
          schema = schema.add("d_"+dim, LongType,true)
          allColumnsNames += "d_"+dim
          if (validDims(dim) == "1") validColumnsNames += "d_"+dim
        }
        case "D" => {
          schema = schema.add("d_"+dim, DoubleType,true)
          allColumnsNames += "d_"+dim
          if (validDims(dim) == "1") validColumnsNames += "d_"+dim
        }
        case "S" => {
          schema = schema.add("d_"+dim, StringType,true)
          allColumnsNames += "d_"+dim
          if (validDims(dim) == "1") validColumnsNames += "d_"+dim
        }
      }
    }
    params.setPropertyValue("allColumnsNames", allColumnsNames.mkString(","))
    params.setPropertyValue("validColumnsNames", validColumnsNames.mkString(","))
    spark.read.format("csv").option("header", "false").option("delimiter", params.getPropertyValue("delimiter"))
      .schema(schema).load(params.inputData)//.rdd

  }

  def calcStatistics(spark: SparkSession, dataset: DataFrame, params: Parameters):DatasetStatistics = {
    val dimsNumStatsAcc = new NumStatsDimAccu()
    spark.sparkContext.register(dimsNumStatsAcc, "dimsNumStatsAcc")
    val dimsNomStatsAcc = new NomLexStatsDimAccu()
    spark.sparkContext.register(dimsNomStatsAcc, "dimsNomStatsAcc")
    val dimsCombStatsAcc = new NomCombCntAccu()
    spark.sparkContext.register(dimsCombStatsAcc, "dimsCombStatsAcc")

    //time=>Time elapsed: 7572106 microsecs then 8775039 for dimsCombStatsAcc
    dataset.foreach(row=> { //row has dimsTypes.length columns, also each row is a multidimensional point
      val dimsTypes = params.getPropertyValue("dimensionTypes").split(",")
      var colsS: String = ""
      for (dim <- 0 to dimsTypes.length-1) {
        dimsTypes(dim) match {
          case "L" => {
            dimsNumStatsAcc.add("d_" + dim, row.getLong(dim)) //although Double is the accum
          }
          case "D" => {
            dimsNumStatsAcc.add("d_" + dim, row.getDouble(dim))
          }
          case "S" => {
            colsS = colsS.concat(","+row.getString(dim))
            dimsNomStatsAcc.add("lex_d_" + dim, row.getString(dim))
          }
          case _ => {}
        }
        //next dim column
      }
      dimsCombStatsAcc.add(colsS.substring(1), 1)
      //next row on worker
    })
    /*
    //time=>Time elapsed: 16103981 microsecs
    val dimsTypes = params.getPropertyValue("dimensionTypes").split(",")
    val validDims = params.getPropertyValue("selectedDimensions").split(",")
    for (dim <- 0 to dimsTypes.length-1) {
      if (validDims(dim)=="1" || validDims(dim)=="0") {//TODO only for valid dims or for all???, for all.
        dimsTypes(dim) match {
          case "L" => {
            dataset.select("d_" + dim).foreach(row => {
              dimsNumStatsAcc.add("min_d_" + dim, row.getLong(0))//although Double is the accum
              dimsNumStatsAcc.add("max_d_" + dim, row.getLong(0))
            })
          }
          case "D" => {
            dataset.select("d_" + dim).foreach(row => {
              dimsNumStatsAcc.add("min_d_" + dim, row.getDouble(0))
              dimsNumStatsAcc.add("max_d_" + dim, row.getDouble(0))
            })
          }
          case "S" => {
            dataset.select("d_" + dim).foreach(row => {
              dimsNomStatsAcc.add("lex_d_" + dim, row.getString(0))
            })
          }
          case _ => {}
        }
      }
    }
    */
    // ["min_d_x","max_d_x","sum_d_x","su2_d_x","cnt_d_x","mea_d_x","var_d_x" -> double],
    // [lex_d_x->[word_w->(word_id-coord, counts)]],
    // ["words combinations" -> counts]
    new DatasetStatistics(dimsNumStatsAcc.value, dimsNomStatsAcc.value, dimsCombStatsAcc.value, dataset.select("d_0").distinct().count(), params)//count run in 200 partitions
  }



}
