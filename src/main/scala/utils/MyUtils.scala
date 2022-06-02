package utils

import org.apache.commons.math3.linear.{ArrayRealVector, BlockRealMatrix}
import org.apache.commons.math3.util.FastMath
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.DatasetStatistics

object MyUtils extends Serializable {

  def jaccardDistance(x: Vector, y: Vector): Double = {
    val xSet = x.toSparse.indices.toSet
    val ySet = y.toSparse.indices.toSet
    val intersectionSize = xSet.intersect(ySet).size.toDouble
    val unionSize = xSet.size + ySet.size - intersectionSize
    assert(unionSize > 0, "The union of two input sets must have at least 1 elements")
    1 - intersectionSize / unionSize
  }

  def prepareDatasetToIdVectorDF(datasetStatistics: DatasetStatistics, datasetInitial: DataFrame): DataFrame = {
    var datasetIdVector = datasetInitial
    var validColumnsNames = datasetStatistics.params.getPropertyValue("validColumnsNames").split(",")
    import org.apache.spark.sql.functions._

    for ( c <- 0 to validColumnsNames.size-1) {
      val validCol = validColumnsNames(c)
      val colType = datasetIdVector.dtypes.filter{ case (name, dtype) => validCol == name }
      if ( colType.size > 0 ) {
        if (colType.head._2 == "StringType") {
          val wordToIdCount = typedLit(datasetStatistics.dimsNomStats.apply("lex_" + validCol))
          datasetIdVector = datasetIdVector.withColumn(validCol + "_vec", wordToIdCount(column(validCol))("_1"))
          // "_1, _2, ...." is the default names of the struct created from map
        }else {
          datasetIdVector = datasetIdVector.withColumn(validCol+"_vec", column(validCol))
        }
      }
    }

    validColumnsNames = validColumnsNames.map(validCol=>validCol+"_vec")
    var datasetReady = new VectorAssembler().setInputCols(validColumnsNames)
      .setOutputCol("features").transform(datasetIdVector)
      .withColumn("rid", concat_ws(",",col("d_0"), col("d_1")))
      .drop(validColumnsNames:_*)

    datasetReady
  }

  def prepareDatasetToIdBinaryVectorDF(datasetStatistics: DatasetStatistics, datasetInitial: DataFrame): DataFrame = {
    var datasetBinary = datasetInitial
    var validColumnsNames = datasetStatistics.params.getPropertyValue("validColumnsNames").split(",")
    import org.apache.spark.sql.functions._
    def rangeizeUDF(dim: String) = udf( (d: Double) => {
      val mind = datasetStatistics.dimsNumStats.apply("min_" + dim)
      val vard = datasetStatistics.dimsNumStats.apply("var_" + dim)
      Math.floor( ((d-mind)/vard) )//indexes on OneHotEncoderEstimator start from 0 to ....
    }, DoubleType)
    for ( c <- 0 to validColumnsNames.size-1) {
      val validCol = validColumnsNames(c)
      val colType = datasetBinary.dtypes.filter{ case (name, dtype) => validCol == name }
      if ( colType.size > 0 ) {
        if (colType.head._2 == "StringType") {
          val wordToIdCount = typedLit(datasetStatistics.dimsNomStats.apply("lex_" + validCol))
          val datasetTemporary = datasetBinary.withColumn(validCol + "_index", wordToIdCount(column(validCol))("_1"))
          // "_1, _2, ...." is the default names of the struct created from map
          datasetBinary = new OneHotEncoderEstimator().setDropLast(false)
            .setInputCols(Array(validCol + "_index")).setOutputCols(Array(validCol + "_vec"))
            .fit(datasetTemporary).transform(datasetTemporary).drop(validCol + "_index") // sparse vector
        } else {
          val datasetTemporary = datasetBinary.withColumn(validCol+"_index", rangeizeUDF(validCol)(column(validCol)))
          datasetBinary = new OneHotEncoderEstimator().setDropLast(false)
            .setInputCols(Array(validCol + "_index")).setOutputCols(Array(validCol + "_vec"))
            .fit(datasetTemporary).transform(datasetTemporary).drop(validCol + "_index") // sparse vector
        }
      }
    }
    validColumnsNames = validColumnsNames.map(validCol=>validCol+"_vec")
    var datasetReady = new VectorAssembler().setInputCols(validColumnsNames)
      .setOutputCol("features").transform(datasetBinary)
      .withColumn("rid", concat_ws(",",col("d_0"), col("d_1")))
      .drop(validColumnsNames:_*)
    datasetReady
  }

  def prepareDatasetToIdVectorRDD(spark: SparkSession, dataset: DataFrame, bcDatasetStats: Broadcast[DatasetStatistics]):
    RDD[(String, Vector)] = {
    dataset.rdd.map(row=>{ //row has dimsTypes.length columns, also each row is a multidimensional point
      var newRowString: Seq[Double] = Seq()
      val dimsTypes = bcDatasetStats.value.params.getPropertyValue("dimensionTypes").split(",")
      val validDims = bcDatasetStats.value.params.getPropertyValue("selectedDimensions").split(",")
      for (dim <- 0 to dimsTypes.length - 1) {
        if (validDims(dim)=="1") {
          dimsTypes(dim) match {
            case "L" => {
              newRowString :+= row.getLong(dim).toDouble
            }
            case "D" => {
              newRowString :+= row.getDouble(dim).toDouble
            }
            case "S" => {
              val newVal = bcDatasetStats.value.dimsNomStats.apply("lex_d_"+dim).apply(row.getString(dim))._1
              newRowString :+= newVal.toDouble
            }
          }
        }
      }
      ( row.getLong(0)+","+row.getLong(1), Vectors.dense(newRowString.toArray) )
    })
  }

  def vectorsRVValue(vector1: Vector, vector2: Vector): Double={
    var v1 = new ArrayRealVector(vector1.toArray)
    var v2 = new ArrayRealVector(vector2.toArray)

    val v1SemiDefine = v1.outerProduct(v1)
    val v2SemiDefine = v2.outerProduct(v2)

    val num = v1SemiDefine.multiply(v2SemiDefine).getTrace
    val denom = FastMath.sqrt( v1SemiDefine.power(2).getTrace * v2SemiDefine.power(2).getTrace )

    if (denom == 0.0){ 0.0 }else{ (num/denom).toDouble }
  }

  def vectorsRVDistance(v1: Vector, v2: Vector): Double ={
    1 - vectorsRVValue(v1, v2)
  }

  def calculateRVDistanceMatrix(vectors: RDD[(String, Vector)]): RDD[(String, String, Double)] = {
    //vectors.repartition(4).cache()
    //vectors.take(1)
    vectors.cartesian(vectors).flatMap{ case(m, om)=> if (m._1 <= om._1) Some( (m._1, om._1, vectorsRVDistance(m._2, om._2)) ) else None }
    //vectors.cartesian(vectors).map{ case(m, om)=> (m._2, om._2, vectorsValue(m._1, om._1)) }
  }

  def main(args: Array[String]): Unit = {
    //as matrices
    val m1: Array[Array[Double]]= Array(
      Array(29.56, -8.78, -20.78, -20.11, 12.89, 7.22),
      Array(-8.78, 2.89, 5.89, 5.56, -3.44, -2.11),
      Array(-20.78, 5.89, 14.89, 14.56, -9.44, -5.11),
      Array(-20.11, 5.56, 14.56, 16.22, -10.78, -5.44),
      Array(12.89, -3.44, -9.44, -10.78, 7.22, 3.56),
      Array(7.22, -2.11, -5.11, -5.44, 3.56, 1.89))

    val m2: Array[Array[Double]]= Array(
      Array(11.81, -3.69, -15.19, -9.69, 8.97, 7.81),
      Array(-3.69, 1.81, 7.31, 1.81, -3.53, -3.69),
      Array(-15.19, 7.31, 34.81, 9.31, -16.03, -20.19),
      Array(-9.69, 1.81, 9.31, 10.81, -6.53, -5.69),
      Array(8.97, -3.53, -16.03, -6.53, 8.14, 8.97),
      Array(7.81, -3.69, -20.19, -5.69, 8.97, 12.81))

    //if not semi-define then another step is needed before
    var v1SemiDefine = new BlockRealMatrix(m1.toVector.toArray)
    var v2SemiDefine = new BlockRealMatrix(m2.toVector.toArray)
    val num = v1SemiDefine.multiply(v2SemiDefine).getTrace
    val denom = FastMath.sqrt( v1SemiDefine.power(2).getTrace * v2SemiDefine.power(2).getTrace )
    println("RVa="+(num/denom).toDouble)

    //as vectors, already semi-define....
    val m1b: Array[Double]= Array(
      29.56, -8.78, -20.78, -20.11, 12.89, 7.22,
      -8.78, 2.89, 5.89, 5.56, -3.44, -2.11,
      -20.78, 5.89, 14.89, 14.56, -9.44, -5.11,
      -20.11, 5.56, 14.56, 16.22, -10.78, -5.44,
      12.89, -3.44, -9.44, -10.78, 7.22, 3.56,
      7.22, -2.11, -5.11, -5.44, 3.56, 1.89)

    val m2b: Array[Double]= Array(
      11.81, -3.69, -15.19, -9.69, 8.97, 7.81,
      -3.69, 1.81, 7.31, 1.81, -3.53, -3.69,
      -15.19, 7.31, 34.81, 9.31, -16.03, -20.19,
      -9.69, 1.81, 9.31, 10.81, -6.53, -5.69,
      8.97, -3.53, -16.03, -6.53, 8.14, 8.97,
      7.81, -3.69, -20.19, -5.69, 8.97, 12.81)

    val v1b = new ArrayRealVector(m1b)
    val v2b = new ArrayRealVector(m2b)
    val numb = v1b.dotProduct(v2b)
    val denomb = FastMath.sqrt( v1b.dotProduct(v1b) * v2b.dotProduct(v2b) )
    println("RVb="+(numb/denomb).toDouble)

  }
}
