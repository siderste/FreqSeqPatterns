package org.apache.spark.mllib.clustering.dbscan.alitouka.util.commandLine

import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.alitouka.spark.dbscan.DbscanSettings

private [dbscan] class CommonArgs (
  var masterUrl: String = null,
  var jar: String = null,
  var inputPath: String = null,
  var outputPath: String = null,
  var distanceMeasure: DistanceMeasure = DbscanSettings.getDefaultDistanceMeasure,
  var debugOutputPath: Option[String] = None)
