package org.apache.spark.mllib.clustering.dbscan.alitouka.util.collection

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable


private [dbscan] class SynchronizedArrayBuffer [T] extends ArrayBuffer[T] with mutable.SynchronizedBuffer[T] {

}
