package preprocess

import scala.collection.concurrent.TrieMap

class DatasetStatistics(inputNumericsMap: TrieMap[String, Double],
                        inputNominalsMap: TrieMap[String, TrieMap[String, (Int, Int)]],
                        inputNumCombMap: TrieMap[String, Long],
                        inputNumOfTrajs: Long,
                          inputParams: Parameters)
  extends Serializable {

  val numOfTrajectories = inputNumOfTrajs

  val params = inputParams
  // ["min_d_x","max_d_x","sum_d_x","su2_d_x","cnt_d_x","mea_d_x","var_d_x" -> double]
  var dimsNumStats = inputNumericsMap
  // "lex_d_x->word_w->(id, count)"
  var dimsNomStats = inputNominalsMap
  // "words" -> Long
  var dimsNomCombCnt = inputNumCombMap
  // "d_" + dim -> countOfCells
  var cellsPerDim: TrieMap[String, Double] = TrieMap()

  def updMinMaxNomDims()={
    //"lex_d_x->word_w->(id, count)"
    dimsNomStats.foreach(lexOfDim=>{
      dimsNumStats.put("min_d_"+lexOfDim._1.substring(6), lexOfDim._2.minBy(_._2._1)._2._1 )
      dimsNumStats.put("max_d_"+lexOfDim._1.substring(6), lexOfDim._2.maxBy(_._2._1)._2._1 )
      dimsNumStats.put("cnt_d_"+lexOfDim._1.substring(6), lexOfDim._2.keys.size )
      // mode in each dim is the max_d_counts
      dimsNumStats.put("mea_d_"+lexOfDim._1.substring(6), lexOfDim._2.maxBy(_._2._2)._2._1 )
    })
  }

  def countCellsPerDim()={
    //called after updMinMaxNomDims
    val validDims = params.getPropertyValue("selectedDimensions").split(",")
    for (dim <- 0 to validDims.length - 1) {
      val cellSize = params.getPropertyValue("cellSizePerDim").split(",")(dim).toDouble// though sizes are int
      val countCells = (dimsNumStats.apply("max_d_" + dim) - dimsNumStats.apply("min_d_" + dim)) / cellSize
      cellsPerDim.put("d_" + dim, countCells.toInt + 1 )//or Math.ceil as cell number covers all points
    }
  }

  override def toString: String = {
    "Dataset Statistics: "+dimsNumStats.mkString("[",", ","]") +";"+ dimsNomStats.mkString("[",", ","]") +";"+
      dimsNomCombCnt.mkString("[",", ","]") +";"+cellsPerDim.mkString("[",", ","]") + ";"+ params.toString
  }
}
