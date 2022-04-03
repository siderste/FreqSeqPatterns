package semantics

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.minhash.{ClusterAggregator, MinHashLSH}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.clustering.kmeans.MyBisectingKMeans
import org.apache.spark.mllib.clustering.msLsh.MsLsh
import org.apache.spark.mllib.clustering.patchwork.PatchWork
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import patterns.{Item, TrajOfItems}
import preprocess.DatasetStatistics

import scala.collection.Map
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.HashMap.HashTrieMap

object SemanticAreas extends Serializable {

  def datasetWithAreaIds(spark: SparkSession, clusteringsMap: Map[String, Int], datasetInitial: Dataset[Row]): DataFrame = {
    import spark.implicits._
    val clusteringLitMap = typedLit(clusteringsMap)
    val pointsToAreaDF = datasetInitial
      .withColumn("rid", concat_ws(",",col("d_0"), col("d_1"))) //although rid already exists
      .withColumn("area_id", clusteringLitMap($"rid") )
      .drop("rid")
    pointsToAreaDF
  }

  def discoverSemAreasFromMinHashLSH(spark: SparkSession, data: DataFrame, datasetStatistics: DatasetStatistics): TrieMap[String, Int] = {
    val mh = new MinHashLSH()
      .setNumHashTables(datasetStatistics.params.getPropertyValue("numOfHashTables").toInt)
      .setInputCol("features")
      .setOutputCol("hashes")
    val model = mh.fit(data)
    val transformedDataset = model.transform(data)
      .select(col("rid"), col("features"), col("hashes"))

    //without self join
    val explodeCols = Seq("entry", "hashValue")
    val transformedDatasetA=transformedDataset.select(col("*"), posexplode(col("hashes")).as(explodeCols))
      .groupBy(col("entry"),col("hashValue")).agg(collect_set(col("rid")).as("rids"))

    //self join on LSH buckets and filter on Jaccard Distance
    val neighboursDF = model.approxSimilarityJoin(transformedDataset,transformedDataset,
      datasetStatistics.params.getPropertyValue("lshJoinThreshold").toDouble,"JaccardDistance").toDF()
    //TODO partition and load balancing?????
    val initialClustersDF = neighboursDF.select(col("entry"),col("hashValue"),col("datasetA.rid").as("ridA"),col("datasetB.rid").as("ridB"))
      .groupBy(col("entry"),col("hashValue")).agg(collect_set(col("ridA")).as("ridAs"),collect_set(col("ridB")).as("ridBs"))
      .select(col("entry"),col("hashValue"),array_union(col("ridAs"),col("ridBs")).as("rids"))

    //either with self join or not, just change:
    // ->>> transformedDatasetA to initialClustersDF -> ok
    //also try repartition with only the hashValue not the entry -> done pretty much the same as hashValue is outnumbering the entry
    //why when doing self-join repartition returns the default 200??? ->
    val clusters = transformedDatasetA.repartition(col("hashValue")).rdd.map(r=>{ //count run in 200 partitions
      val entry = r.getAs[Int]("entry")
      val hashValue=r.getAs[DenseVector]("hashValue")
      val rids=r.getAs[collection.mutable.WrappedArray[String]]("rids")
      //((entry, hashValue),(rids.toSet,Set[String]()))
      rids.toSet
    }).aggregate(new ClusterAggregator())(//also try treeAggregate    aggregate though almost the same -> done
      seqOp = (agg, v) => agg.add(v),
      combOp = (agg1, agg2) => agg1.merge(agg2)
      //,2
    )
    //return non zero sets with every element having an id attached as Map[String, Int]
    clusters.all_clusters_toMap()

    /* OLD STUFF
    //process neighbors to clusters
    //TODO costly operation, check partitioning....
    val clusteringsMapAcc = new ClusteringsMapAccumulator()
    spark.sparkContext.register(clusteringsMapAcc, "clusteringsMapAcc")
    var clusterSize = clusteringsMapAcc.value.keys.size

    neighboursDF//.repartition(spark.sparkContext.defaultParallelism) //TODO partition and load balancing?????
      .foreachPartition{ partition=>partition
        .foreach(row=>{
          clusteringsMapAcc.add(row.getStruct(0).getAs[String]("rid"),
            row.getStruct(1).getAs[String]("rid") )
        })
      }

    clusteringsMapAcc.value
    */
  }


  def discoverSemAreasFromMeanShiftLSH(spark: SparkSession, data: RDD[(String, Vector)],
                                       bcDatasetStatistics: Broadcast[DatasetStatistics]): Map[String, Int] = {
    val params = bcDatasetStatistics.value.params
    val meanShift = MsLsh
    //apply the MS LSH algorithm
    val model = meanShift.train(spark.sparkContext, data, k=7, epsilon1=0.05,
      epsilon2=0.05, epsilon3=0.07.toDouble, ratioToStop=1.0, yStarIter=10.toInt, cmin=3.toInt,
      normalisation="true".toBoolean, w=1, nbseg=7, nbblocs1=10.toInt, nbblocs2=5.toInt, nbLabelIter=1.toInt)

    //get clusters of each vector as map
    model.head.labelizedRDD.map{case(clid,(vecid,vec))=>{
      (vecid, clid)  }}.collectAsMap() // not memory efficient
  }


  def buildSemanticGrid(spark: SparkSession, dataset: DataFrame, bcDatasetStatistics: Broadcast[DatasetStatistics])
  : (TrieMap[String, Int], TrieMap[String, Int], DataFrame) = {

    import spark.implicits._
    val semCellsIdsMapAcc = new StingToAutoIdMapAccumulator()
    spark.sparkContext.register(semCellsIdsMapAcc, "semCellsIdsMapAcc")
    val semCellsCountsMapAcc = new StringToTotalCountsMapAccumulator()
    spark.sparkContext.register(semCellsCountsMapAcc, "semCellsCountsMapAcc")
    val pointToCellSeqAcc = new PointToCellSeqAccumulator()
    spark.sparkContext.register(pointToCellSeqAcc, "pointToCellSeqAcc")

    dataset.foreach(row=>{ //row has dimsTypes.length columns, also each row is a multidimensional point
      var cell: String = ""
      var pointsDims: String = ""
      val dimsTypes = bcDatasetStatistics.value.params.getPropertyValue("dimensionTypes").split(",")
      val validDims = bcDatasetStatistics.value.params.getPropertyValue("selectedDimensions").split(",")
      for (dim <- 0 to dimsTypes.length - 1) {
        //TODO only for valid dims or for all???
        if (validDims(dim)=="1") {
          dimsTypes(dim) match {
            case "L" => {
              val dimCellSize = bcDatasetStatistics.value.params.getPropertyValue("cellSizePerDim").split(",")(dim).toLong
              val coord = ((row.getLong(dim) - bcDatasetStatistics.value.dimsNumStats.apply("min_d_"+dim)) / dimCellSize ).toInt
              //or coord = Math.floor( row.getLong(dim) / dimCellSize ).toInt //as cells start from 0,....
              cell = if (cell.length == 0) coord.toString else cell + "," + coord
              pointsDims = if (pointsDims.length == 0) row.getLong(dim).toString else pointsDims + "," + row.getLong(dim)
            }
            case "D" => {
              val dimCellSize = bcDatasetStatistics.value.params.getPropertyValue("cellSizePerDim").split(",")(dim).toDouble
              val coord = ((row.getDouble(dim) - bcDatasetStatistics.value.dimsNumStats.apply("min_d_"+dim)) / dimCellSize ).toInt
              //or coord = Math.floor( row.getDouble(dim) / dimCellSize ).toInt //as cells start from 0,....
              cell = if (cell.length == 0) coord.toString else cell + "," + coord
              pointsDims = if (pointsDims.length == 0) row.getDouble(dim).toString else pointsDims + "," + row.getDouble(dim)
            }
            case "S" => {
              val coord = bcDatasetStatistics.value.dimsNomStats.apply("lex_d_"+dim).apply(row.getString(dim))._1
              // or for negative ...-lex_d size (not to use the 0)
              cell = if (cell.length == 0) coord.toString else cell +  "," + coord
              pointsDims = if (pointsDims.length == 0) coord.toDouble.toString else pointsDims + "," + coord.toDouble
            }
          }
        }
      }
      //if ( row.getLong(0) == 5993235 ){println("cell="+cell+", point="+row.toSeq.toString())} //test a trajectory
      semCellsIdsMapAcc.add(cell)
      semCellsCountsMapAcc.add((cell, 1))
      pointToCellSeqAcc.add( (row.getLong(0), row.getLong(1), cell, pointsDims) )//transform row
    })
    //all workers done
    // {cellCoords->cell_id, cellCoords->counts, Seq(tid, time, cellCoords, pointsDimValues).join on dataset}
    ( semCellsIdsMapAcc.value, semCellsCountsMapAcc.value,
      pointToCellSeqAcc.value.toDF("tid","time","cell","pointsdim")
        .join(dataset.withColumnRenamed("d_0", "tid").withColumnRenamed("d_1", "time"),
          Seq("tid", "time"))
      //TODO Time elapsed: 11287917 microsecs with it, and Time elapsed: 8886293 microsecs without it.
    )
  }

  def discoverSemAreasFromPatchwork(spark: SparkSession, data: RDD[(String, Int)], datasetStatistics: DatasetStatistics,
                                    semanticCellsCounts: TrieMap[String, Int]): TrieMap[String, Int]={

    val cellsToAreaIdMapAcc = new StingToGivenIdMapAccumulator()
    spark.sparkContext.register(cellsToAreaIdMapAcc, "cellsToAreaIdMapAcc")

    val clustererPatch  = new PatchWork(datasetStatistics.params.getPropertyValue("epsilonFlag").split(",").map(x=>x.toDouble),
      datasetStatistics.params.getPropertyValue("minPtsFlag").toInt, datasetStatistics.params.getPropertyValue("ratioFlag").toDouble,
      datasetStatistics.params.getPropertyValue("minCellInClusterFlag").toInt )

    val patchworkModel = clustererPatch.runAlgorithmOnCells(data.cache())

    data.foreach(cell => {
      cellsToAreaIdMapAcc.add( cell._1, patchworkModel.predict(cell._1).getID )
    })

    cellsToAreaIdMapAcc.value
  }

  def transformToGAreasSeqsRDD(spark: SparkSession, datasetWithAreasDF: DataFrame): RDD[(Long, Array[Array[Item]])] ={
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //TODO check efficiency
    datasetWithAreasDF.select("d_0","d_1","area_id")
      .withColumn("itemsArr", struct("d_1","area_id") )
      .groupBy("d_0").agg(sort_array(collect_list("itemsArr")).as("items") )
      //.as[TrajOfItems].as("trajOfItems")
      // "array(time","area_id")"
      .rdd.map(row=>{ (row.getLong(0), row.getAs[collection.mutable.WrappedArray[Row]]("items").toArray.map( r=>{Array(Item(r.getLong(0), r.getInt(1)))} ) ) })
  }

  def transformToAreasSeqsRDD(spark: SparkSession, datasetWithAreasDF: DataFrame): RDD[ (Long, Array[Array[Item]]) ] ={
    import org.apache.spark.sql.functions._
    //TODO check efficiency, map partitions
    datasetWithAreasDF.select("d_0","d_1","area_id").repartition(col("d_0")).rdd.map(r=>{
      ( r.getLong(0), Array(Item(r.getLong(1), r.getInt(2))) )
    }).reduceByKey{(array_1, array_2)=>
      Array.concat(array_1, array_2).sorted
    }.mapValues(m=>Array(m))
  }

  def discoverSemAreasFromBisectingKMeans(spark: SparkSession, data: RDD[(String, Vector)],
                                          bcDatasetStatistics: Broadcast[DatasetStatistics]): HashTrieMap[String, Int] = {

    val bkm = new MyBisectingKMeans(bcDatasetStatistics.value.params.getPropertyValue("bskm_k").toInt,
      bcDatasetStatistics.value.params.getPropertyValue("bskm_maxIterations").toInt,
      bcDatasetStatistics.value.params.getPropertyValue("bskm_minDivisibleClusterSize").toInt,
      classOf[MyBisectingKMeans].getName.##,
      bcDatasetStatistics.value.params.getPropertyValue("bskm_distanceMethod")
    )

    val model = bkm.myRun(data, bcDatasetStatistics)

    model.root.leafNodes.zipWithIndex.flatMap{ case (node, idx) =>
      node.members.map( m=>( m._1, idx+1 ) )// as 0 is used in prefixspan as separators
    }.toMap.asInstanceOf[HashTrieMap[String, Int]]
  }

}
