package org.apache.spark.mllib.clustering.kmeans

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

object BisectingKMeansExample {

  def main(args: Array[String]): Unit = {
    //to debug start it like the following and remove provided from build.sbt, also mind paths
    val spark = SparkSession.builder.master("local").appName("FreqSeqPatterns").getOrCreate()

    // Loads and parses data
    def parse(line: String): Vector = Vectors.dense(line.split(" ").map(_.toDouble))
    val data = spark.sparkContext.textFile("input/kmeans_data.txt").map(parse).cache()

    // Clustering the data into 6 clusters by BisectingKMeans.
    val bkm = new BisectingKMeans().setK(3)
    val model = bkm.run(data)

    val bkm2 = new MyBisectingKMeans(3, 20, 1.0,
      classOf[MyBisectingKMeans].getName.##, MyDistanceMeasure.RV)
    //val model2 = bkm2.myRun(data)

    // Show the compute cost and the cluster centers
    println(s"Compute Cost: ${model.computeCost(data)}")
    model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }
/*
    println(s"Compute Cost: ${model.computeCost(data)}")
    model2.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }
*/
    System.in.read()
    spark.stop()
  }
}
