package org.apache.spark.mllib.clustering.dbscan.irvingc

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import utils.MyUtils

case class DBSCANPoint(val vector: (String, Vector) ) {

  def id = vector._1
  def vec = vector._2
  def x = vector._2(0)
  def y = vector._2(1)
  def dim = vector._2.size

  def distanceSquared(other: DBSCANPoint, method: String): Double = {
    //val dx = other.x - x
    //val dy = other.y - y
    //(dx * dx) + (dy * dy)
    method match {
      case "EU" => {Vectors.sqdist(this.vec, other.vec)}
      case "RV" => {MyUtils.vectorsRVDistance(this.vec, other.vec)}
      case _ => {throw new UnsupportedOperationException(s"Vector distance method not specified.")}
    }
  }

}
