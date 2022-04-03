package org.apache.spark.mllib.clustering.dbscan.alitouka.spatial

import org.alitouka.spark.dbscan.util.collection.SynchronizedArrayBuffer

private [dbscan] class BoxTreeItemWithPoints (b: Box,
  val points: SynchronizedArrayBuffer[Point] = new SynchronizedArrayBuffer[Point] (),
  val adjacentBoxes: SynchronizedArrayBuffer[BoxTreeItemWithPoints] = new SynchronizedArrayBuffer[BoxTreeItemWithPoints] ())
  extends BoxTreeItemBase [BoxTreeItemWithPoints] (b) {
}
