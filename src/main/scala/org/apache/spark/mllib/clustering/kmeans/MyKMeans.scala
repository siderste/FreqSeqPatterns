package org.apache.spark.mllib.clustering.kmeans

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.clustering.{DistanceMeasure, KMeans, KMeansModel, LocalKMeans, VectorWithNorm}
import org.apache.spark.mllib.linalg.BLAS.axpy
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ArrayBuffer

class MyKMeans(
                var k: Int,
                var maxIterations: Int,
                var initializationMode: String,
                var initializationSteps: Int,
                var epsilon: Double,
                var seed: Long,
                var myDistanceMeasure: String
              ) extends KMeans{

  var initialModel: Option[KMeansModel] = None

  def initRandom(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    // Select without replacement; may still produce duplicates if the data has < k distinct
    // points, so deduplicate the centroids to match the behavior of k-means|| in the same situation
    data.takeSample(false, k, new XORShiftRandom(this.seed).nextInt())
      .map(_.vector).distinct.map(new VectorWithNorm(_))
  }

  def myRun(data: RDD[Vector]): MyKMeansModel = {
    myRun(data, None)
  }

  def myRun(
           data: RDD[Vector],
           instr: Option[Instrumentation]): MyKMeansModel = {

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Compute squared norms and cache them.
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map { case (v, norm) =>
      new VectorWithNorm(v, norm)
    }
    val model = runAlgorithm(zippedData, instr)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  def runAlgorithm(
    data: RDD[VectorWithNorm],
    instr: Option[Instrumentation]): MyKMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val distanceMeasureInstance = MyDistanceMeasure.decodeFromString(this.myDistanceMeasure)

    val centers = initialModel match {
      case Some(kMeansCenters) =>
        kMeansCenters.clusterCenters.map(new VectorWithNorm(_))
      case None =>
        if (initializationMode == KMeans.RANDOM) {
          initRandom(data)
        } else {
          initKMeansParallel(data, distanceMeasureInstance)
        }
    }
    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(f"Initialization with $initializationMode took $initTimeInSeconds%.3f seconds.")

    var converged = false
    var cost = 0.0
    var iteration = 0

    val iterationStartTime = System.nanoTime()

    instr.foreach(_.logNumFeatures(centers.head.vector.size))

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && !converged) {
      val costAccum = sc.doubleAccumulator
      val bcCenters = sc.broadcast(centers)

      // Find the new centers
      val collected = data.mapPartitions { points =>
        val thisCenters = bcCenters.value
        val dims = thisCenters.head.vector.size

        val sums = Array.fill(thisCenters.length)(Vectors.zeros(dims))
        val counts = Array.fill(thisCenters.length)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = distanceMeasureInstance.findClosest(thisCenters, point)
          costAccum.add(cost)
          distanceMeasureInstance.updateClusterSum(point, sums(bestCenter))
          counts(bestCenter) += 1
        }

        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        axpy(1.0, sum2, sum1)
        (sum1, count1 + count2)
      }.collectAsMap()

      if (iteration == 0) {
        instr.foreach(_.logNumExamples(collected.values.map(_._2).sum))
      }

      val newCenters = collected.mapValues { case (sum, count) =>
        distanceMeasureInstance.centroid(sum, count)
      }

      bcCenters.destroy(blocking = false)

      // Update the cluster centers and costs
      converged = true
      newCenters.foreach { case (j, newCenter) =>
        if (converged &&
          !distanceMeasureInstance.isCenterConverged(centers(j), newCenter, epsilon)) {
          converged = false
        }
        centers(j) = newCenter
      }

      cost = costAccum.value
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    logInfo(s"The cost is $cost.")

    new MyKMeansModel(centers.map(_.vector), myDistanceMeasure, cost, iteration)
  }

  override def initKMeansParallel(data: RDD[VectorWithNorm],
                                  distanceMeasureInstance: DistanceMeasure): Array[VectorWithNorm] = {
    // Initialize empty centers and point costs.
    var costs = data.map(_ => Double.PositiveInfinity)

    // Initialize the first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(false, 1, seed)
    // Could be empty if data is empty; fail with a better message early:
    require(sample.nonEmpty, s"No samples available from $data")

    val centers = ArrayBuffer[VectorWithNorm]()
    var newCenters = Seq(sample.head.toDense)
    centers ++= newCenters

    // On each step, sample 2 * k points on average with probability proportional
    // to their squared distance from the centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    val bcNewCentersList = ArrayBuffer[Broadcast[_]]()
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      bcNewCentersList += bcNewCenters
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        math.min(distanceMeasureInstance.pointCost(bcNewCenters.value, point), cost)
      }.persist(StorageLevel.MEMORY_AND_DISK)
      val sumCosts = costs.sum()

      bcNewCenters.unpersist(blocking = false)
      preCosts.unpersist(blocking = false)

      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointCosts.filter { case (_, c) => rand.nextDouble() < 2.0 * c * k / sumCosts }.map(_._1)
      }.collect()
      newCenters = chosen.map(_.toDense)
      centers ++= newCenters
      step += 1
    }

    costs.unpersist(blocking = false)
    bcNewCentersList.foreach(_.destroy(false))

    val distinctCenters = centers.map(_.vector).distinct.map(new VectorWithNorm(_))

    if (distinctCenters.size <= k) {
      distinctCenters.toArray
    } else {
      // Finally, we might have a set of more than k distinct candidate centers; weight each
      // candidate by the number of points in the dataset mapping to it and run a local k-means++
      // on the weighted centers to pick k of them
      val bcCenters = data.context.broadcast(distinctCenters)
      val countMap = data
        .map(distanceMeasureInstance.findClosest(bcCenters.value, _)._1)
        .countByValue()

      bcCenters.destroy(blocking = false)

      val myWeights = distinctCenters.indices.map(countMap.getOrElse(_, 0L).toDouble).toArray
      MyLocalKMeans.kMeansPlusPlus(0, distinctCenters.toArray, myWeights, k, 30)
    }
  }

}


