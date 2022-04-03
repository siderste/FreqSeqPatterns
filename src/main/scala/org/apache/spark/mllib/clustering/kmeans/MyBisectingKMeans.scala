package org.apache.spark.mllib.clustering.kmeans

import java.util.Random

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.clustering.{BisectingKMeans, DistanceMeasure, VectorWithNorm}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import preprocess.{DatasetStatistics, Parameters}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MyBisectingKMeans(
      private var k: Int,
      private var maxIterations: Int,
      private var minDivisibleClusterSize: Double,
      private var seed: Long,
      private var distanceMeasure: String
      ) extends BisectingKMeans {

  import MyBisectingKMeans._

  def myRun(input: RDD[(String, Vector)], bcDatasetStats: Broadcast[DatasetStatistics] ): MyBisectingKMeansModel = {
    myRun(input, None, bcDatasetStats)
  }

  def myRun(
             input: RDD[(String, Vector)],
             instr: Option[Instrumentation],
             bcDatasetStats: Broadcast[DatasetStatistics] ): MyBisectingKMeansModel = {
    if (input.getStorageLevel == StorageLevel.NONE) {
      logWarning(s"The input RDD ${input.id} is not directly cached, which may hurt performance if"
        + " its parent RDDs are also not cached.")
    }
    val d = input.map(_._2.size).first()
    logInfo(s"Feature dimension: $d.")

    val dMeasure = MyDistanceMeasure.decodeFromString(this.distanceMeasure)
    // Compute and cache vector norms for fast distance computation.
    val norms = input.map(v => Vectors.norm(v._2, 2.0)).persist(StorageLevel.MEMORY_AND_DISK)
    val vectors = input.zip(norms).map { case (x, norm) => ( x._1, new VectorWithNorm(x._2, norm) )}
    var assignments = vectors.map(v => (ROOT_INDEX, v))
    var activeClusters = summarize(d, assignments, dMeasure, bcDatasetStats.value.params)
    instr.foreach(_.logNumExamples(activeClusters.values.map(_.size).sum))
    val rootSummary = activeClusters(ROOT_INDEX)
    val n = rootSummary.size
    logInfo(s"Number of points: $n.")
    logInfo(s"Initial cost: ${rootSummary.cost}.")
    val minSize = if (minDivisibleClusterSize >= 1.0) {
      math.ceil(minDivisibleClusterSize).toLong
    } else {
      math.ceil(minDivisibleClusterSize * n).toLong
    }
    logInfo(s"The minimum number of points of a divisible cluster is $minSize.")
    var inactiveClusters = mutable.Seq.empty[(Long, ClusterSummary)]
    val random = new Random(seed)
    var numLeafClustersNeeded = k - 1
    var level = 1
    var preIndices: RDD[Long] = null
    var indices: RDD[Long] = null
    while (activeClusters.nonEmpty && numLeafClustersNeeded > 0 && level < LEVEL_LIMIT) {
      // Divisible clusters are sufficiently large and have non-trivial cost.
      var divisibleClusters = activeClusters.filter { case (_, summary) =>
        (summary.size >= minSize) && (summary.cost > MLUtils.EPSILON * summary.size)
      }
      // If we don't need all divisible clusters, take the larger ones.
      if (divisibleClusters.size > numLeafClustersNeeded) {
        divisibleClusters = divisibleClusters.toSeq.sortBy { case (_, summary) =>
          -summary.size
        }.take(numLeafClustersNeeded)
          .toMap
      }
      if (divisibleClusters.nonEmpty) {
        val divisibleIndices = divisibleClusters.keys.toSet
        logInfo(s"Dividing ${divisibleIndices.size} clusters on level $level.")
        var newClusterCenters = divisibleClusters.flatMap { case (index, summary) =>
          val (left, right) = splitCenter(summary.center, random, dMeasure)
          Iterator((leftChildIndex(index), left), (rightChildIndex(index), right))
        }.map(identity) // workaround for a Scala bug (SI-7005) that produces a not serializable map
        var newClusters: Map[Long, ClusterSummary] = null
        var newAssignments: RDD[(Long, (String, VectorWithNorm))] = null
        for (iter <- 0 until maxIterations) {
          newAssignments = updateAssignments(assignments, divisibleIndices, newClusterCenters,
            dMeasure)
            .filter { case (index, _) =>
              divisibleIndices.contains(parentIndex(index))
            }
          newClusters = summarize(d, newAssignments, dMeasure, bcDatasetStats.value.params)
          newClusterCenters = newClusters.mapValues(_.center).map(identity)
        }
        if (preIndices != null) {
          preIndices.unpersist(false)
        }
        preIndices = indices
        indices = updateAssignments(assignments, divisibleIndices, newClusterCenters, dMeasure).keys
          .persist(StorageLevel.MEMORY_AND_DISK)
        assignments = indices.zip(vectors)
        inactiveClusters ++= activeClusters
        activeClusters = newClusters
        numLeafClustersNeeded -= divisibleClusters.size
      } else {
        logInfo(s"None active and divisible clusters left on level $level. Stop iterations.")
        inactiveClusters ++= activeClusters
        activeClusters = Map.empty
      }
      level += 1
    }
    if (preIndices != null) {
      preIndices.unpersist(false)
    }
    if (indices != null) {
      indices.unpersist(false)
    }
    norms.unpersist(false)
    val clusters = activeClusters ++ inactiveClusters
    val root = buildTree(clusters, dMeasure)
    new MyBisectingKMeansModel(root, this.distanceMeasure)
  }

}

object MyBisectingKMeans extends Serializable {

  /** The index of the root node of a tree. */
  private val ROOT_INDEX: Long = 1

  private val MAX_DIVISIBLE_CLUSTER_INDEX: Long = Long.MaxValue / 2

  private val LEVEL_LIMIT = math.log10(Long.MaxValue) / math.log10(2)

  /** Returns the left child index of the given node index. */
  private def leftChildIndex(index: Long): Long = {
    require(index <= MAX_DIVISIBLE_CLUSTER_INDEX, s"Child index out of bound: 2 * $index.")
    2 * index
  }

  /** Returns the right child index of the given node index. */
  private def rightChildIndex(index: Long): Long = {
    require(index <= MAX_DIVISIBLE_CLUSTER_INDEX, s"Child index out of bound: 2 * $index + 1.")
    2 * index + 1
  }

  /** Returns the parent index of the given node index, or 0 if the input is 1 (root). */
  private def parentIndex(index: Long): Long = {
    index / 2
  }

  /**
    * Summarizes data by each cluster as Map.
    * @param d feature dimension
    * @param assignments pairs of point and its cluster index
    * @return a map from cluster indices to corresponding cluster summaries
    */
  private def summarize(
                         d: Int,
                         assignments: RDD[( Long, (String, VectorWithNorm) )],
                         distanceMeasure: DistanceMeasure,
                         params: Parameters): Map[Long, ClusterSummary] = {
    assignments.aggregateByKey(new ClusterSummaryAggregator(d, distanceMeasure, params))(
      seqOp = (agg, v) => agg.add(v),
      combOp = (agg1, agg2) => agg1.merge(agg2)
    ).mapValues(_.summary)
      .collect().toMap
  }

  /**
    * Cluster summary aggregator.
    * @param d feature dimension
    */
  private class ClusterSummaryAggregator( val d: Int, val distanceMeasure: DistanceMeasure,
           params: Parameters )
    extends Serializable {
    private var n: Long = 0L
    private var members: ListBuffer[(String, VectorWithNorm)] = ListBuffer()
    private var clusterCost: Double = 0.0
    private val sum: Vector = Vectors.zeros(d)
    private var sumSq: Double = 0.0

    /** Adds a point. */
    def add(v: (String, VectorWithNorm) ): this.type = {
      n += 1L
      members += v
      // TODO: use a numerically stable approach to estimate cost
      sumSq += v._2.norm * v._2.norm
      distanceMeasure.updateClusterSum(v._2, sum)
      this
    }

    /** Merges another aggregator. */
    def merge(other: ClusterSummaryAggregator): this.type = {
      n += other.n
      members ++= other.members
      sumSq += other.sumSq
      distanceMeasure.updateClusterSum(new VectorWithNorm(other.sum), sum)
      this
    }

    def getClusterCost(distanceMeasure: DistanceMeasure): Double = {
      var cost: Double = 0.0
      for (v1 <- 0 to members.length - 1){
        for (v2 <- 0 to members.length - 1){
          if (v1 < v2){
            cost += distanceMeasure.distance( members(v1)._2, members(v2)._2 )
          }
        }
      }
      cost
    }

    /** Returns the summary. */
    def summary: ClusterSummary = {
      //val center = getCentroidVectorWithTextDim(params)
      val center = distanceMeasure.centroid(sum.copy, n)
      //val cost = getClusterCost(distanceMeasure)
      val cost = distanceMeasure.clusterCost(center, new VectorWithNorm(sum), n, sumSq)
      ClusterSummary(n, members.toArray, center, cost)
    }

    def getCentroidVectorWithTextDim(params: Parameters): VectorWithNorm = {
      var cendroid: Array[Double] = Array()
      var i: Int = -1
      var total: Double = 0.0
      var semValues: mutable.HashMap[Double, Int] = mutable.HashMap.empty
      val dimsTypes = params.getPropertyValue("dimensionTypes").split(",")
      val validDims = params.getPropertyValue("selectedDimensions").split(",")
      for (dim <- 0 to dimsTypes.length - 1) {
        if (validDims(dim)=="1") {
          i += 1
          dimsTypes(dim) match {
            case "L" => {
              total = 0.0
              members.foreach(m=>{ total += m._2.vector(i) })
              cendroid :+= total/members.length
            }
            case "D" => {
              total = 0.0
              members.foreach(m=>{ total += m._2.vector(i) })
              cendroid :+= total/members.length
            }
            case "S" => {
              semValues = mutable.HashMap.empty
              members.foreach(m=>{
                if (semValues.contains(m._2.vector(i))) semValues.put(m._2.vector(i), semValues.apply(m._2.vector(i)) + 1)
                else semValues.put(m._2.vector(i), 1)
              })
              cendroid :+= semValues.maxBy{ _._2 }._1
            }
          }
        }
      }
      new VectorWithNorm(cendroid)
    }

  }

  /**
    * Bisects a cluster center.
    *
    * @param center current cluster center
    * @param random a random number generator
    * @return initial centers
    */
  private def splitCenter(
                           center: VectorWithNorm,
                           random: Random,
                           distanceMeasure: DistanceMeasure): (VectorWithNorm, VectorWithNorm) = {
    val d = center.vector.size
    val norm = center.norm
    val level = 1e-4 * norm
    val noise = Vectors.dense(Array.fill(d)(random.nextDouble()))
    distanceMeasure.symmetricCentroids(level, noise, center.vector)
  }

  /**
    * Updates assignments.
    * @param assignments current assignments
    * @param divisibleIndices divisible cluster indices
    * @param newClusterCenters new cluster centers
    * @return new assignments
    */
  private def updateAssignments(
                                 assignments: RDD[(Long, (String, VectorWithNorm))],
                                 divisibleIndices: Set[Long],
                                 newClusterCenters: Map[Long, VectorWithNorm],
                                 distanceMeasure: DistanceMeasure): RDD[(Long, (String, VectorWithNorm))] = {
    assignments.map { case (index, v) =>
      if (divisibleIndices.contains(index)) {
        val children = Seq(leftChildIndex(index), rightChildIndex(index))
        val newClusterChildren = children.filter(newClusterCenters.contains)
        val newClusterChildrenCenterToId =
          newClusterChildren.map(id => newClusterCenters(id) -> id).toMap
        val newClusterChildrenCenters = newClusterChildrenCenterToId.keys.toArray
        if (newClusterChildren.nonEmpty) {
          val selected = distanceMeasure.findClosest(newClusterChildrenCenters, v._2)._1
          val center = newClusterChildrenCenters(selected)
          val id = newClusterChildrenCenterToId(center)
          (id, v)
        } else {
          (index, v)
        }
      } else {
        (index, v)
      }
    }
  }

  /**
    * Builds a clustering tree by re-indexing internal and leaf clusters.
    * @param clusters a map from cluster indices to corresponding cluster summaries
    * @return the root node of the clustering tree
    */
  private def buildTree(
                         clusters: Map[Long, ClusterSummary],
                         distanceMeasure: DistanceMeasure): ClusteringTreeNode = {
    var leafIndex = 0
    var internalIndex = -1

    /**
      * Builds a subtree from this given node index.
      */
    def buildSubTree(rawIndex: Long): ClusteringTreeNode = {
      val cluster = clusters(rawIndex)
      val size = cluster.size
      val center = cluster.center
      val cost = cluster.cost
      val members = cluster.members
      val isInternal = clusters.contains(leftChildIndex(rawIndex))
      if (isInternal) {
        val index = internalIndex
        internalIndex -= 1
        val leftIndex = leftChildIndex(rawIndex)
        val rightIndex = rightChildIndex(rawIndex)
        val indexes = Seq(leftIndex, rightIndex).filter(clusters.contains)
        val height = indexes.map { childIndex =>
          distanceMeasure.distance(center, clusters(childIndex).center)
        }.max
        val children = indexes.map(buildSubTree).toArray
        new ClusteringTreeNode(index, size, center, cost, height, children, members)
      } else {
        val index = leafIndex
        leafIndex += 1
        val height = 0.0
        new ClusteringTreeNode(index, size, center, cost, height, Array.empty, members)
      }
    }

    buildSubTree(ROOT_INDEX)
  }

  /**
    * Summary of a cluster.
    *
    * @param size the number of points within this cluster
    * @param center the center of the points within this cluster
    * @param cost the sum of squared distances to the center
    */
  private case class ClusterSummary(size: Long, members: Array[(String, VectorWithNorm)], center: VectorWithNorm, cost: Double)


}

private[clustering] class ClusteringTreeNode private[clustering] (
     val index: Int,
     val size: Long,
     private[clustering] val centerWithNorm: VectorWithNorm,
     val cost: Double,
     val height: Double,
     val children: Array[ClusteringTreeNode],
     val members: Array[(String, VectorWithNorm)]) extends Serializable {

  /** Whether this is a leaf node. */
  val isLeaf: Boolean = children.isEmpty

  require((isLeaf && index >= 0) || (!isLeaf && index < 0))

  /** Cluster center. */
  def center: Vector = centerWithNorm.vector

  /** Predicts the leaf cluster node index that the input point belongs to. */
  def predict(point: Vector, distanceMeasure: DistanceMeasure): Int = {
    val (index, _) = predict(new VectorWithNorm(point), distanceMeasure)
    index
  }

  /** Returns the full prediction path from root to leaf. */
  def predictPath(point: Vector, distanceMeasure: DistanceMeasure): Array[ClusteringTreeNode] = {
    predictPath(new VectorWithNorm(point), distanceMeasure).toArray
  }

  /** Returns the full prediction path from root to leaf. */
  private def predictPath(
                           pointWithNorm: VectorWithNorm,
                           distanceMeasure: DistanceMeasure): List[ClusteringTreeNode] = {
    if (isLeaf) {
      this :: Nil
    } else {
      val selected = children.minBy { child =>
        distanceMeasure.distance(child.centerWithNorm, pointWithNorm)
      }
      selected :: selected.predictPath(pointWithNorm, distanceMeasure)
    }
  }

  /**
    * Computes the cost of the input point.
    */
  def computeCost(point: Vector, distanceMeasure: DistanceMeasure): Double = {
    val (_, cost) = predict(new VectorWithNorm(point), distanceMeasure)
    cost
  }

  /**
    * Predicts the cluster index and the cost of the input point.
    */
  private def predict(
                       pointWithNorm: VectorWithNorm,
                       distanceMeasure: DistanceMeasure): (Int, Double) = {
    predict(pointWithNorm, distanceMeasure.cost(centerWithNorm, pointWithNorm), distanceMeasure)
  }

  /**
    * Predicts the cluster index and the cost of the input point.
    * @param pointWithNorm input point
    * @param cost the cost to the current center
    * @return (predicted leaf cluster index, cost)
    */
  @tailrec
  private def predict(
                       pointWithNorm: VectorWithNorm,
                       cost: Double,
                       distanceMeasure: DistanceMeasure): (Int, Double) = {
    if (isLeaf) {
      (index, cost)
    } else {
      val (selectedChild, minCost) = children.map { child =>
        (child, distanceMeasure.cost(child.centerWithNorm, pointWithNorm))
      }.minBy(_._2)
      selectedChild.predict(pointWithNorm, minCost, distanceMeasure)
    }
  }

  /**
    * Returns all leaf nodes from this node.
    */
  def leafNodes: Array[ClusteringTreeNode] = {
    if (isLeaf) {
      Array(this)
    } else {
      children.flatMap(_.leafNodes)
    }
  }
}

