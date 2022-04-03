package org.apache.spark.mllib.clustering.kmeans

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

object KMeansExample {

  def main(args: Array[String]): Unit = {
    //to debug start it like the following and remove provided from build.sbt, also mind paths
    val spark = SparkSession.builder.master("local").appName("FreqSeqPatterns").getOrCreate()

    // Load and parse the data
    val data = spark.sparkContext.textFile("input/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clustersO = KMeans.train(parsedData, numClusters, numIterations)
    val clusters = new MyKMeans(numClusters, numIterations, KMeans.K_MEANS_PARALLEL, 2,
      1e-4, Utils.random.nextLong(), MyDistanceMeasure.RV).myRun(parsedData)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    clusters.save(spark.sparkContext, "target/org/apache/spark/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(spark.sparkContext, "target/org/apache/spark/KMeansExample/KMeansModel")

    System.in.read()
    spark.stop()
  }
}
