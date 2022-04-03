package patterns

import org.apache.spark.mllib.fpm.{PrefixSpan, PrefixSpanModel}
import org.apache.spark.mllib.clustering.dbscan.irvingc.{DBSCANLabeledPoint, DBSCANPoint, LocalDBSCANNaive}
import org.apache.spark.mllib.clustering.meanshift.{DenseDoubleVector, DoubleVector, LocalMeanShift, PointWithTrajid}
import org.apache.spark.mllib.clustering.msLsh.MsLsh
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, flatten, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import preprocess.Parameters

import java.util.List
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

object FreqSeqPatterns {

  def clusterTransitionsLocalMeanShift(patternTransitionsRDD: RDD[((String, Int), mutable.Map[Long, Array[Vector]])], params: Parameters)
  : RDD[((String, Int), mutable.Map[Integer, List[PointWithTrajid]] )] = {
    //params should be passed as broadcast in mapValues
    //val maxIteration = params.getPropertyValue("maxIteration").toInt
    //val mergeWindow = params.getPropertyValue("mergeWindow").toDouble
    //val bandwidth = params.getPropertyValue("bandwidth").toDouble

    patternTransitionsRDD.mapValues{case transitionsVectors =>
      import collection.JavaConverters._
      case class Point(vec: DoubleVector, tid: Long)
      val points = transitionsVectors.flatMap{case v=>
        v._2.map(vec=>{
          var newVec: DoubleVector = new DenseDoubleVector(vec.toArray)
          new PointWithTrajid(newVec, v._1)
        })
      }.toList //id is missed here
      val localMeanShift = new LocalMeanShift()
      val maxIteration = params.getPropertyValue("maxIteration").toInt
      val mergeWindow = params.getPropertyValue("mergeWindow").toDouble
      val bandwidth = params.getPropertyValue("bandwidth").toDouble
      val weights = points.map(p=>1.asInstanceOf[Integer]).toList; // weights of points
      val centers = localMeanShift.cluster(points.asJava, weights.asJava, bandwidth, mergeWindow, maxIteration)
      val clusters = localMeanShift.getClusters.asScala
      clusters
    }
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


  def getVectorTransitionsToCluster(spark: SparkSession, datasetPatternsTransitions: DataFrame)
    :RDD[((String, Int), mutable.Map[Long, Array[Vector]])]= {
    //TODO costly????
    import spark.implicits._
    val t = datasetPatternsTransitions.select($"d_0",flatten($"pattern"),$"pattern",$"pattern_length",$"points_length",$"point_coords").take(10)

    datasetPatternsTransitions.rdd.map(row=>{
      var pattern = row.getAs[collection.mutable.WrappedArray[collection.mutable.WrappedArray[Int]]]("pattern")
        .toArray.map(r=>r.toArray.mkString("")).mkString(",")
      var traj_id = row.getAs[Long]("d_0")
      var pointsAll = row.getAs[collection.mutable.WrappedArray[collection.mutable.WrappedArray[Double]]]("point_coords")
        .toArray.map(r=> r.toArray)
      var pattern_length = row.getAs[Int]("pattern_length")
      var points_length = row.getAs[Int]("points_length")
      var transitionsVectors = (1 to (points_length/pattern_length)).map{i=>
        var newPointsarray = ArrayBuffer[Double]()
        pointsAll.slice((i-1)*pattern_length, i*pattern_length).foreach{point=>
          newPointsarray ++= point
        }
        Vectors.dense(newPointsarray.toArray)
      }
      var setOfTrajIds = Set(traj_id)
      var mapTrajid=mutable.Map.empty[Long, Array[Vector]]
      mapTrajid.put(traj_id,transitionsVectors.toArray)
      ((pattern, pattern_length), mapTrajid)
    }).reduceByKey{ case(mapTrajidA, mapTrajidB)=>
      var allKeys = mapTrajidA.keys.toSet++mapTrajidB.keys.toSet //trajectory ids
      allKeys.foreach(key=>{
        if(mapTrajidB.contains(key) && mapTrajidA.contains(key)){
          var newarray = mapTrajidA(key)++mapTrajidB(key)
          mapTrajidA(key)=newarray
        }else if(!mapTrajidA.contains(key)){
          mapTrajidA(key)=mapTrajidB(key)
        }else if(!mapTrajidB.contains(key)){

        }
      })
      mapTrajidA
    }
  }


  //def getTransitionsDFToCluster(spark: SparkSession, prefixSpanModel: MyPrefixSpanModel, datasetWithAreasDF: DataFrame) = { //old getTransitionsDFToCluster
  def getTransitionsDFToCluster(spark: SparkSession, prefixSpanRows: RDD[Row], datasetWithAreasDF: DataFrame) = {
    //val frequentPatterns = prefixSpanModel.freqSequences.filter(f=>f.instances.nonEmpty) //taking care inside MyPrefixSpan
    /*
    val freqPatternsRow = prefixSpanModel.freqSequences.flatMap(freqSequence =>{
      freqSequence.instances.keys.flatMap(sequenceid=>{
        freqSequence.instances(sequenceid).map(time=>{
          Row(sequenceid, time, freqSequence.sequence.length, freqSequence.freq, freqSequence.sequence)
        })
      })
    })
    */
    val freqPatternsSchema = new StructType()
      .add("d_0", LongType, true).add("d_1", LongType, true)
      .add("pattern_length", IntegerType, true).add("pattern_freq", LongType, true)
      .add("pattern", ArrayType(ArrayType(IntegerType)), true)

    val freqPatternsDF = spark.createDataFrame(prefixSpanRows, freqPatternsSchema)

    import org.apache.spark.sql.functions._
    val t1 = freqPatternsDF.join(datasetWithAreasDF, Seq("d_0","d_1"))
      .withColumn("point_coords",array("d_2","d_3","d_4"))
      .select("pattern","d_0","d_1","point_coords","pattern_length")

      //here if i group d_0 by point_coords then i have for each point_coords (transition) the number of visitors. Is it worthy???
    //val t2 = t1.groupBy("point_coords").agg(collect_list("d_0"))

    t1.groupBy("pattern","pattern_length","d_0").agg(sort_array(collect_list(struct("d_1","point_coords"))).as("timespoints"))
      .withColumn("points_length",size(col("timespoints")))
      .select("pattern","d_0","pattern_length","points_length","timespoints.point_coords")
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
  def discoverFreqSeqPatterns( data: RDD[(Long, Array[Array[Item]])], params: Parameters):  RDD[Row]={
    val minSupport = params.getPropertyValue("minSupport").toDouble
    val maxPatternLength = params.getPropertyValue("maxPatternLength").toInt
    val maxLocalProjDBSize = params.getPropertyValue("maxLocalProjDBSize").toLong
    val maxDTofPatterns = params.getPropertyValue("maxDTofPatterns").toInt

    //val sequences1 = data.select("trajOfItems").where(col("trajOfItems").isNotNull)
    //val sequences = sequences1.rdd.map(r => (r.getAs[Long]("d_0"), r.getAs[collection.mutable.WrappedArray[Item]](1).toArray.map(item => Array(item))))  ///.cache()

    val sequences=data
    new MyPrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(maxLocalProjDBSize)
      .setMaxDTofPatterns(10000000)
      .run(sequences)
  }

  def discoverFreqSeqPatternsOrig( sequences: RDD[ (Long, Array[Array[Item]]) ], params: Parameters):  PrefixSpanModel[Int]={
    val minSupport = params.getPropertyValue("minSupport").toDouble
    val maxPatternLength = params.getPropertyValue("maxPatternLength").toInt
    val maxLocalProjDBSize = params.getPropertyValue("maxLocalProjDBSize").toLong

    val sequencesAreas = sequences.map{r=>(
      r._2.map{ar=>(ar.map{i=>(i.area_id)} )} )}

    new PrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(maxLocalProjDBSize)
      .run[Int](sequencesAreas.cache())
  }
}
