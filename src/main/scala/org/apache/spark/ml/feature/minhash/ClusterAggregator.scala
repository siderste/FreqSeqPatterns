package org.apache.spark.ml.feature.minhash

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

class ClusterAggregator extends Serializable {

  var all_clusters: ListBuffer[Set[String]] = ListBuffer[Set[String]]()

  /** Adds a new cluster. */
  def add(v: Set[String] ): this.type = {
    if (v.nonEmpty) {
      var v_found = false
      var tmp_set: Set[String] = Set[String]()
      var new_clusters: ListBuffer[Set[String]] = ListBuffer[Set[String]]()
      for (i <- all_clusters.indices) {
        val intersct = all_clusters.apply(i).intersect(v)
        if (intersct.nonEmpty) {
          v_found = true
          tmp_set = tmp_set | all_clusters.apply(i)
        } else {
          new_clusters.+=(all_clusters.apply(i))
        }
      }
      if (v_found) tmp_set = tmp_set | v else new_clusters.+=(v)
      all_clusters = new_clusters.+=(tmp_set)
    }
    this
  }

  /** Merges another aggregator. */
  def merge(other: ClusterAggregator): this.type = {
    val other_all_clusters = other.all_clusters
    for( i <- other_all_clusters.indices){
      if (other_all_clusters.apply(i).nonEmpty)
        this.add(other_all_clusters.apply(i))
    }
    this
  }

  //TODO costly??memory??
  def all_clusters_toMap(): TrieMap[String, Int] ={
    var m: TrieMap[String, Int] = TrieMap().empty
    var clusters_id = all_clusters.zipWithIndex
    for( i <- clusters_id.indices){
      var cur_cluster = clusters_id.apply(i)
      if (cur_cluster._1.size>0)
        cur_cluster._1.foreach(v=>m.put(v,cur_cluster._2))
    }
    m
  }

}

