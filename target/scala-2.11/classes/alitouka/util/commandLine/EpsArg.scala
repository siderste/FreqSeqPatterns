package org.apache.spark.mllib.clustering.dbscan.alitouka.util.commandLine

import org.alitouka.spark.dbscan.DbscanSettings

private [dbscan] trait EpsArg {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
