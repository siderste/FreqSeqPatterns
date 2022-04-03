/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  * <p/>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p/>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

/**
  * @author Beck Gaël
  */

package org.apache.spark.mllib.clustering.msLsh

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class Mean_shift_lsh_model(val clustersCenter:Map[Int, Vector], val clustersCardinalities:scala.collection.Map[Int, Long],
                           val labelizedRDD:RDD[(Int, (String, Vector))] ,val maxMinArray:(Array[Double], Array[Double])) extends Serializable
{
  def numCluster : Int = clustersCenter.size

  def predict(point:Vector) : Int = MsLsh.prediction(point, clustersCenter)

  def predict(points:Array[Vector]) : ListBuffer[Int] = {
    val res = ListBuffer.empty[Int]
    for( ind <- 0 until points.size) res += MsLsh.prediction(points(ind), clustersCenter)
    res
  }
}