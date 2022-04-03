package org.apache.spark.mllib.clustering.dbscan.alitouka.util.commandLine

private [dbscan] trait NumberOfPointsInPartitionParsing [C <: CommonArgs with NumberOfPointsInPartitionArg] extends CommonArgsParser[C] {
  opt[Long] ("npp")
    .foreach { args.numberOfPoints = _ }

}


