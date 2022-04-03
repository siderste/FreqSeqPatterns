package org.apache.spark.mllib.clustering.dbscan.alitouka.spatial

import org.alitouka.spark.dbscan._

private [dbscan] class BoxIdGenerator (val initialId: BoxId) {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
