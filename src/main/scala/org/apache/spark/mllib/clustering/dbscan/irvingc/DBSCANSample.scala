package org.apache.spark.mllib.clustering.dbscan.irvingc

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object DBSCANSample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DBSCAN Sample")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src" )

    val parsedData = data.zipWithIndex().map{case(s, idx) => (idx.toString, Vectors.dense(s.split(',').map(_.toDouble)))}.cache()
    val eps = 2.5
    val minPoints = 10
    val maxPointsPerPartition = 500

    println("EPS: "+eps+" minPoints: "+minPoints )

    val model = DBSCAN.train(
      parsedData,
      eps = eps,
      minPoints = minPoints,
      maxPointsPerPartition = maxPointsPerPartition)

    model.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}").saveAsTextFile("dest")

    sc.stop()
  }
}
