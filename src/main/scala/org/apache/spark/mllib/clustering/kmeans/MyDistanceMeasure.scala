package org.apache.spark.mllib.clustering.kmeans

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.clustering.{CosineDistanceMeasure, DistanceMeasure, EuclideanDistanceMeasure, VectorWithNorm}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.Vector
import utils.MyUtils

class MyDistanceMeasure extends DistanceMeasure {

  override def distance(v1: VectorWithNorm, v2: VectorWithNorm): Double = {
    ???
  }

  override def clusterCost(centroid: VectorWithNorm, pointsSum: VectorWithNorm,
                           numberOfPoints: Long, pointsSquaredNorm: Double): Double = {
    ???
  }
}

object MyDistanceMeasure {

  @Since("2.4.0")
  val EUCLIDEAN = "euclidean"
  @Since("2.4.0")
  val COSINE = "cosine"
  val JACCARD = "jaccard"
  val RV = "RV"

  private[spark] def decodeFromString(distanceMeasure: String): DistanceMeasure =
    distanceMeasure match {
      case EUCLIDEAN => new EuclideanDistanceMeasure
      case COSINE => new CosineDistanceMeasure
      //case JACCARD => new JaccardDistanceMeasure
      case RV => new RVDistanceMeasure
      case _ => throw new IllegalArgumentException(s"distanceMeasure must be one of: " +
        s"$EUCLIDEAN, $COSINE. $distanceMeasure provided.")
    }

  private[spark] def validateDistanceMeasure(distanceMeasure: String): Boolean = {
    distanceMeasure match {
      case DistanceMeasure.EUCLIDEAN => true
      case DistanceMeasure.COSINE => true
      case MyDistanceMeasure.RV => true
      case _ => false
    }
  }
}

class RVDistanceMeasure extends MyDistanceMeasure {

  override def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      //var lowerBoundOfSqDist = center.norm - point.norm
      //lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      //if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = RVDistanceMeasure.fastDistance(center, point)//EuclideanDistanceMeasure.fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      //}
      i += 1
    }
    (bestIndex, bestDistance)
  }

  override def isCenterConverged(
      oldCenter: VectorWithNorm,
      newCenter: VectorWithNorm,
      epsilon: Double): Boolean = {
    RVDistanceMeasure.fastDistance(newCenter, oldCenter) <= epsilon * epsilon
  }

  override def distance(v1: VectorWithNorm, v2: VectorWithNorm): Double = {
    RVDistanceMeasure.fastDistance(v1, v2)
  }

  override def clusterCost(
      centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      numberOfPoints: Long,
      pointsSquaredNorm: Double): Double = {
    math.max(pointsSquaredNorm - numberOfPoints * centroid.norm * centroid.norm, 0.0)
  }

  override def cost(
      point: VectorWithNorm,
      centroid: VectorWithNorm): Double = {
    RVDistanceMeasure.fastDistance(point, centroid)
  }

  override def updateClusterSum(point: VectorWithNorm, sum: Vector): Unit = {
    axpy(1.0, point.vector, sum)
  }

  override def centroid(sum: Vector, count: Long): VectorWithNorm = {
    scal(1.0 / count, sum)
    new VectorWithNorm(sum)
  }

  override def symmetricCentroids(
                          level: Double,
                          noise: Vector,
                          centroid: Vector): (VectorWithNorm, VectorWithNorm) = {
    val left = centroid.copy
    axpy(-level, noise, left)
    val right = centroid.copy
    axpy(level, noise, right)
    (new VectorWithNorm(left), new VectorWithNorm(right))
  }

}

object RVDistanceMeasure {

  private[clustering] def fastDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    //MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
    MyUtils.vectorsRVDistance(v1.vector, v2.vector)
  }
}

