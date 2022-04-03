package org.apache.spark.mllib.clustering.msLsh

import org.apache.spark.mllib.clustering.msLsh
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Main {
  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder.master("local").appName("FreqSeqPatterns").getOrCreate() //for local debug
    //val sc = spark.sparkContext//for local debug
    val sc = new SparkContext(new SparkConf)

    val meanShift = MsLsh

    val data = sc.textFile("data/ais_brest_day1_1_13rows.csv").map(_.split(";").map(_.toDouble))
      .zipWithIndex
      .map{ case(data, id) => (id.toString, Vectors.dense(data))}.cache

    val model = meanShift.train(sc, data, k=2, epsilon1=0.005.toDouble, epsilon2=0.005.toDouble, epsilon3=0.007.toDouble,
      ratioToStop=1.0, yStarIter=10.toInt, cmin=1.toInt, normalisation="true".toBoolean, w=1, nbseg=100,
      nbblocs1=3.toInt, nbblocs2=2.toInt, nbLabelIter=1.toInt)


    val nbd = data.count
    val nbd2 = model.head.clustersCardinalities.values.reduce(_+_)

    println("nbd : " + nbd + "\nnbd2 : " + nbd2)

    model.head.clustersCardinalities.foreach(println)
    //meanShift.savelabeling(model(0),"/myPath/label")
    //meanShift.saveClusterInfo(model(0),"/myPath/clusterInfo")

  }

  def runMS(sc: SparkContext, datasetin: RDD[(Array[Double], Long)], args: Array[String], distMethod: String): ArrayBuffer[Mean_shift_lsh_model] = {

    val meanShift = MsLsh

    val data = datasetin
      .map{ case(data, id) => (id.toString, Vectors.dense(data))}.cache

    val model = meanShift.train(sc, data, k=15, epsilon1=args(0).toDouble, epsilon2=args(1).toDouble, epsilon3=args(2).toDouble,
      ratioToStop=1.0, yStarIter=args(3).toInt, cmin=args(4).toInt, normalisation=args(5).toBoolean, w=1, nbseg=100,
      nbblocs1=args(6).toInt, nbblocs2=args(7).toInt, nbLabelIter=args(8).toInt)


    val nbd = data.count
    val nbd2 = model.head.clustersCardinalities.values.reduce(_+_)

    println("nbd : " + nbd + "\nnbd2 : " + nbd2)

    //model.head.clustersCardinalities.foreach(println)
    //meanShift.savelabeling(model(0),"/myPath/label")
    //meanShift.saveClusterInfo(model(0),"/myPath/clusterInfo")
    model
  }
}
