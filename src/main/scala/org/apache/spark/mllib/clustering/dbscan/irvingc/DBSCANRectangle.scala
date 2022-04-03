package org.apache.spark.mllib.clustering.dbscan.irvingc

/**
 * A rectangle with a left corner and a right upper corner
 */
case class DBSCANRectangle(x: Double, y: Double, x2: Double, y2: Double) {

  /**
   * Returns whether other is contained by this box//TODO is 2d
   */
  def contains(other: DBSCANRectangle): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }

  /**
   * Returns whether point is contained by this box//TODO is 2d
   */
  def contains(point: DBSCANPoint): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2
  }

  /**
   * Returns a new box from shrinking this box by the given amount
   */
  def shrink(amount: Double): DBSCANRectangle = {
    DBSCANRectangle(x + amount, y + amount, x2 - amount, y2 - amount)
  }

  /**
   * Returns a whether the rectangle contains the point, and the point
   * is not in the rectangle's border
   */
  def almostContains(point: DBSCANPoint): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2
  }

}
