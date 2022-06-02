package patterns

import org.apache.spark.mllib.clustering.dbscan.irvingc.{DBSCANLabeledPoint, DBSCANPoint, LocalDBSCANNaive}
import org.apache.spark.mllib.clustering.meanshift.{DenseDoubleVector, DoubleVector, LocalMeanShift, PointWithTrajid}
import org.apache.spark.mllib.clustering.msLsh.MsLsh
import org.apache.spark.mllib.fpm.{PrefixSpan, PrefixSpanModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import preprocess.Parameters

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

object FreqSeqPatterns {

  def clusterTransitionsLocalMeanShift(
                                        patternTransitionsRDD: RDD[((String, Int), Map[Long, Array[Vector]])],
                                        //patternTransitionsRDD: RDD[((String, Int), mutable.Map[Long, Array[Vector]])],
                                       params: Parameters, spark: SparkSession)
     : RDD[((String, Int), Map[Integer, Array[String]] )] = {

    implicit val myObjEncoder: Encoder[((String, Int), mutable.Map[Integer, java.util.List[PointWithTrajid]])]
    = org.apache.spark.sql.Encoders.kryo[((String, Int), mutable.Map[Integer, java.util.List[PointWithTrajid]])]

    patternTransitionsRDD.map(patternAndTransitionsVectors => {
      import collection.JavaConverters._
      case class Point(vec: DoubleVector, tid: Long)
      val points = patternAndTransitionsVectors._2.flatMap { case v =>
        v._2.map(vec => {
          var newVec: DoubleVector = new DenseDoubleVector(vec.toArray)
          new PointWithTrajid(newVec, v._1)
        })
      }.toList
      val localMeanShift = new LocalMeanShift()
      val maxIteration = params.getPropertyValue("maxIteration").toInt
      val mergeWindow = params.getPropertyValue("mergeWindow").toDouble
      val bandwidth = params.getPropertyValue("bandwidth").toDouble
      val weights = points.map(p => 1.asInstanceOf[Integer]).toList; // weights of points
      val centers = localMeanShift.cluster(points.asJava, weights.asJava, bandwidth, mergeWindow, maxIteration)
      val clusters = localMeanShift.getClusters.asScala
        .map(tpoints=>(tpoints._1,tpoints._2.asScala.toArray.map(p=>p.getId+"-"+p.getVec.toArray.mkString(","))))
      (patternAndTransitionsVectors._1, clusters)
    }) ///(myObjEncoder)
  }

  def clusterTransitionsLocalDBSCAN(patternTransitionsRDD: RDD[((String, Int), mutable.Map[Long, Array[Vector]])])
    : RDD[((String, Int), Array[DBSCANLabeledPoint] )] = {
    patternTransitionsRDD.mapValues{case transitionsVectors =>
      var points = transitionsVectors.zipWithIndex.flatMap{case(v, i)=>{
        var id = v._1.toString+"-"+i.toString
        v._2.map(vec=>{
          DBSCANPoint(id, vec)
        })
      }}
      var eps = 100//TODO calc per pattern transitions
      var minPoints = 2//TODO calc per pattern transitions
      var clusteringPoints = new LocalDBSCANNaive(eps, minPoints).fit(points)
      clusteringPoints.toArray
    }
  }


  def getVectorTransitionsToCluster(spark: SparkSession, datasetPatternsTransitions: RDD[((Array[Array[Int]], Int), Map[Long, Array[Vector]])])
    :RDD[((String, Int), Map[Long, Array[Vector]])]= {
    //:Dataset[((String, Int), mutable.Map[Long, Array[Vector]])]= {
    //TODO costly????
    //slicing points of each trajectory of each pattern and glue them to transitions
    datasetPatternsTransitions.map(row=>{
      var pattern = row._1._1.toArray.map(r=>r.toArray.mkString("")).mkString(",")
      var pattern_length = row._1._2
      val transitionsMap = row._2.map(m=>{
        var traj_id = m._1
        var pointsAll = m._2
        var points_length = pointsAll.length
        var transitionsVectors = (1 to (points_length/pattern_length)).map{i=>
          var newPointsarray = ArrayBuffer[Double]()
          pointsAll.slice((i-1)*pattern_length, i*pattern_length).foreach{point=>
            newPointsarray ++= point.toArray
          }
          Vectors.dense(newPointsarray.toArray)
        }
        (traj_id,transitionsVectors.toArray)
      }).toMap
      ((pattern, pattern_length), transitionsMap)
    })  //.rdd
  }

  def clusterTransitionsMeanShiftLSH(spark: SparkSession, data: RDD[(String, Vector)]): Map[Vector, Int] = {
    //var k_neigh = //TODO calc
    val meanShift = MsLsh
    //apply the MS LSH algorithm
    val model = meanShift.train(spark.sparkContext, data, k=5, epsilon1=0.05.toDouble,
      epsilon2=0.05.toDouble, epsilon3=0.07.toDouble, ratioToStop=1.0, yStarIter=10.toInt, cmin=3.toInt,
      normalisation="true".toBoolean, w=1, nbseg=7, nbblocs1=10.toInt, nbblocs2=5.toInt, nbLabelIter=1.toInt)

    //get clusters of each vector as map
    model.head.labelizedRDD.map{case(clid,(vecid,vec))=>{
      (vec, clid)  }}.collectAsMap() // not memory efficient
  }

  //def discoverFreqSeqPatterns( data: RDD[(Long, Array[Array[Item]])], params: Parameters):  MyPrefixSpanModel={ // old discoverFreqSeqPatterns
  def discoverFreqSeqPatterns( data: Dataset[(Long, Array[Array[Item]])], params: Parameters, spark: SparkSession):  RDD[((Array[Array[Int]], Int), scala.collection.Map[Long, Array[Vector]])]={
    val minSupport = params.getPropertyValue("minSupport").toDouble
    val maxPatternLength = params.getPropertyValue("maxPatternLength").toInt
    val maxLocalProjDBSize = params.getPropertyValue("maxLocalProjDBSize").toLong
    val maxDTofPatterns = params.getPropertyValue("maxDTofPatterns").toInt

    //val sequences1 = data.select("trajOfItems").where(col("trajOfItems").isNotNull)
    //val sequences = sequences1.rdd.map(r => (r.getAs[Long]("d_0"), r.getAs[collection.mutable.WrappedArray[Item]](1).toArray.map(item => Array(item))))  ///.cache()

    val sequences=data.cache()
    new MyPrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(maxLocalProjDBSize)
      .setMaxDTofPatterns(maxDTofPatterns)
      .run(sequences, spark)

  }

  def discoverFreqSeqPatternsOrig( sequences: Dataset[ (Long, Array[Array[Item]]) ], params: Parameters):  PrefixSpanModel[Int]={
    val minSupport = params.getPropertyValue("minSupport").toDouble
    val maxPatternLength = params.getPropertyValue("maxPatternLength").toInt
    val maxLocalProjDBSize = params.getPropertyValue("maxLocalProjDBSize").toLong

    implicit val myObjEncoder: Encoder[Array[Array[Int]]]
    = org.apache.spark.sql.Encoders.kryo[Array[Array[Int]]]

    val sequencesAreas = sequences.map{r=> r._2.map{ ar=> ar.map{ i=> i.area_id } } }(myObjEncoder)

    new PrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(maxLocalProjDBSize)
      .run[Int](sequencesAreas.rdd.cache())
  }
}
