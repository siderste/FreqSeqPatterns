// stylianos

import database.PostgreSQL
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.clustering.dbscan.irvingc.DBSCAN
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import patterns.FreqSeqPatterns
import preprocess.{DataPreprocessing, DatasetStatistics, Parameters}
import semantics.SemanticAreas
import utils.MyUtils

import scala.collection.mutable

object MainDraft extends Serializable{
  // Creates a SparkSession
  val spark = SparkSession.builder.appName("FreqSeqPatterns")
    .config("spark.executor.heartbeatInterval", "30000s")
    .config("spark.network.timeout", "60000s")
    .getOrCreate()

  def main(args: Array[String]) {

    //read application properties
    val params = new Parameters(getClass.getResource("application.properties"))
    //get postgresql ready
    val postgres = new PostgreSQL(spark)
    postgres.setUpConnection() //comment if not used

    //Step 1: preprocessing
    var timings = mutable.Map.empty[String, Long]
    var start=System.nanoTime()
    var datasetInitial = DataPreprocessing.readInput(spark, params)//.repartition(10).cache()
      //.limit(2000)
    //clean the database from:

    val datasetStatistics = DataPreprocessing.calcStatistics(spark, datasetInitial, params)
    datasetStatistics.updMinMaxNomDims()
    datasetStatistics.countCellsPerDim()
    //println("datasetStatistics="+datasetStatistics.toString)
    val bcDatasetStatistics: Broadcast[DatasetStatistics] = spark.sparkContext.broadcast(datasetStatistics)
    //val dataset2 = DataPreprocessing.normalizeData(datasetInitial, datasetStatistics)
    //TODO align data in time, start all from 0 timestamp ???
    timings.put("step1", (System.nanoTime()-start)/1000000000)

    //Step 2: discover semantic areas
    //Step 2a: build semantic grid, assign each point to cell
    // functions for input common to all methods

    //val (semanticCells, semanticCellsCounts, datasetWithAreasDF) = SemanticAreas.buildSemanticGrid(spark, datasetInitial, bcDatasetStatistics)
    //val bcSemanticCells: Broadcast[TrieMap[String, Int]] = spark.sparkContext.broadcast(semanticCells)//TODO bc not needed for now

    //Step 2b: cluster input points to discover areas
    start=System.nanoTime()
    val datasetWithAreasDF: DataFrame = params.getPropertyValue("semAreasMethod") match {
      case "MINHASHLSH" => {
        //prepare dataset
        var data = MyUtils.prepareDatasetToIdBinaryVectorDF(datasetStatistics, datasetInitial) //eg data.where("rid='7329986,1356'").collect()
        //apply spark minhash LSH
        var clusteringsMap = SemanticAreas.discoverSemAreasFromMinHashLSH(spark, data, datasetStatistics)// eg clusteringsMap("7329986,1356") --> AreaId
        //return datasetInitial with area_id column for each point
        var pointsToAreaDF = SemanticAreas.datasetWithAreaIds(spark, clusteringsMap, datasetInitial)// eg pointsToAreaDF.where("d_0=7329986 and d_1=1356").collect()
        pointsToAreaDF//.persist()
        //throw new NotImplementedError("operation not yet implemented")
      }
      case "MSLSH" => {
        //make an id based on d_0 and d_1 key columns and prepare dataset
        /*val data = MyUtils.prepareDatasetToIdVectorDF(datasetStatistics, datasetInitial)
          .select("rid", "features").rdd.map(row=>{(row.getString(0), Vectors.fromML(row.getAs[Vector](1)) )  })*/
        //var data = MyUtils.prepareDatasetToIdVectorRDD(spark,datasetInitial,bcDatasetStatistics)
        var data = MyUtils.prepareDatasetToIdBinaryVectorDF(datasetStatistics, datasetInitial)
          .select("rid","features").rdd.map(row=>{
          ( row.getString(0), Vectors.fromML(row.getAs[Vector](1)) )
        })
        //MyUtils.prepareDatasetToIdVectorDF=1857413418 //MyUtils.prepareDatasetToIdVectorRDD=11747389//MyUtils.prepareDatasetToIdBinaryVectorDF.map=6548341760
        //apply spark meanshift LSH
        var clusteringsMap = SemanticAreas.discoverSemAreasFromMeanShiftLSH(spark, data, bcDatasetStatistics)
        //return datasetInitial with area_id column for each point
        var pointsToAreaDF = SemanticAreas.datasetWithAreaIds(spark, clusteringsMap, datasetInitial)
        pointsToAreaDF//.show(false)
        //throw new NotImplementedError("operation not yet implemented")
      }
      case "CELLS" => {
        // using no discovery
        //semanticCells
        throw new NotImplementedError("operation not yet implemented")
      }
      case "PATCHWORK" => {
        // using PatchWork
        //SemanticAreas.discoverSemAreasFromPatchwork(spark, spark.sparkContext.parallelize(semanticCellsCounts.toList),
        // datasetStatistics, semanticCellsCounts)
        throw new NotImplementedError("operation not yet implemented")
      }
      case "BISECTING" =>{
        //using Bisecting KMeans
        //val data = MyUtils.prepareDatasetToIdVectorRDD(spark, datasetInitial, bcDatasetStatistics)
        var data = MyUtils.prepareDatasetToIdBinaryVectorDF(datasetStatistics, datasetInitial)
          .select("rid","features").rdd.map(row=>{
          ( row.getString(0), Vectors.fromML(row.getAs[Vector](1)) )
        })
        val clusteringsMap = SemanticAreas.discoverSemAreasFromBisectingKMeans(spark, data, bcDatasetStatistics)
        //return datasetInitial with area_id column for each point
        var pointsToAreaDF = SemanticAreas.datasetWithAreaIds(spark, clusteringsMap, datasetInitial)
        pointsToAreaDF//.show(false)
        //throw new NotImplementedError("operation not yet implemented")
      }
      case "DBSCAN" => {
        // using RV
        val data = MyUtils.prepareDatasetToIdVectorRDD(spark, datasetInitial, bcDatasetStatistics)
        //TODO this should go to preprocessing, maybe
        val model = DBSCAN.train(data, 0.7, 10, 500)
        //model.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}")
        throw new NotImplementedError("operation not yet implemented")
      }
      case "HOTSPOT" => {
        // using HotSpot Getis-Ord score and merge cells to discover areas TODO not yet implemented
        //val getisOrdStats = Analyzer.calculateGetisOrdStatistics(spark, semanticCellsCounts, semanticCellsCounts.size) //bc it???
        throw new NotImplementedError("operation not yet implemented")
      }
      case "MERGINGCELLS" => {
        // using merge cells into areas some way to discover areas TODO
        throw new NotImplementedError("operation not yet implemented")
      }
      case _ => {
        throw new UnsupportedOperationException("no suitable method for discovering areas found")
      }
    }
    timings.put("step2b", (System.nanoTime()-start)/1000000000)
    //comment out
    postgres.writeOverwriteToDB(datasetWithAreasDF,"localhost","45436","siderDatacron","public","datasetWithAreasDF") // till here-> 252.432 s
    start=System.nanoTime()
    //Step 2c: transform datasetInitial from points of dims sequences to (time, areas) sequences for prefixSpan
    val sequences = SemanticAreas.transformToGAreasSeqsRDD(spark, datasetWithAreasDF) ///.persist() // + 6.728 s
    //TODO merge nearBy-sequential Areas???
    timings.put("step2c", (System.nanoTime()-start)/1000000000)

    start=System.nanoTime()
    //Step 3: discover frequent sequential semantic areas patterns, along with pathlets of each
    val prefixSpanPatternsRDD = FreqSeqPatterns.discoverFreqSeqPatterns(sequences, bcDatasetStatistics.value.params, spark) // + 6.728 s
    //val prefixSpanPatternsRDD = FreqSeqPatterns.discoverFreqSeqPatternsOrig(sequences, bcDatasetStatistics.value.params)
    //TODO what about Dt, inside prefixspan or segment trajectories in preprocessing???
    timings.put("step3", (System.nanoTime()-start)/1000000000)

    start=System.nanoTime()
    //val datasetPatternsTransitions2 = spark.read.table("TableName");
    val patternTransitionsRDD = FreqSeqPatterns.getVectorTransitionsToCluster(spark, prefixSpanPatternsRDD)
    //comment out
    postgres.writeOverwriteToDB(postgres.writeOverwriteRDDToDB(patternTransitionsRDD),"localhost","45436","siderDatacron","public","patternTransitionsRDD")
    timings.put("step4a", (System.nanoTime()-start)/1000000000)

    start=System.nanoTime()
    //val patternsAndClusters = FreqSeqPatterns.clusterTransitionsLocalDBSCAN(patternTransitionsRDD)
    val patternsAndClusters2 = FreqSeqPatterns.clusterTransitionsLocalMeanShift(patternTransitionsRDD, bcDatasetStatistics.value.params, spark)
    //comment out
    postgres.writeOverwriteToDB(postgres.writeOverwriteRDDClustersToDB(patternsAndClusters2),"localhost","45436","siderDatacron","public","patternCLustersRDD")
    val clustersArray = patternsAndClusters2.collect()
    patternsAndClusters2.take(1000 )
    timings.put("step4b", (System.nanoTime()-start)/1000000000)

    println("Done in (secs):"+timings.toString())
    throw new NotImplementedError("operation not yet implemented. END")
  }
}
