/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package patterns

import org.apache.log4j.LogManager
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Calculate all patterns of a projected database in local mode.
  *
  * @param minCount minimal count for a frequent pattern
  * @param maxPatternLength max pattern length for a frequent pattern
  */
private[patterns] class MyLocalPrefixSpan(
                                      val minCount: Long,
                                      val maxPatternLength: Int) extends Logging with Serializable {
  import MyLocalPrefixSpan.ReversedPrefix
  import MyPrefixSpan.{Postfix, Prefix}

  /**
    * Generates frequent patterns on the input array of postfixes.
    * @param postfixes an array of postfixes
    * @return an iterator of (frequent pattern, count)
    */
  def run(postfixes: Array[Postfix], maxDTofPatterns: Int): Iterator[(ReversedPrefix, Long)] = {
    genFreqPatterns(ReversedPrefix.empty, postfixes, maxDTofPatterns).map { case (prefix, count) =>
      (prefix, count)// toSequence adds a 0 in start of prefix and reverse it e.g. 1 0 => 0 1 0
    }
  }

  /** PrefixSpan original
    * Recursively generates frequent patterns.
    * @param prefix current prefix
    * @param postfixes projected postfixes w.r.t. the prefix
    * @return an iterator of (prefix, count)
    */
  private def genFreqPatterns(
                               prefix: ReversedPrefix,
                               postfixes: Array[Postfix],
                               maxDTofPatterns: Int): Iterator[(ReversedPrefix, Long)] = {
    if (maxPatternLength == prefix.length || postfixes.length < minCount) {
      return Iterator.empty
    }
    //var log = LogManager.getRootLogger
    // find frequent items
    val counts = mutable.Map.empty[Int, mutable.Map[Long, Set[Long]]] // counts(prefix->(seqid->[times]))
    postfixes.foreach { postfix =>
      postfix.genPrefixItems.foreach { case (x, sizeAndInstances) =>
        //log.warn("d found prefix="+x+", when in ReversedPrefix="+prefix+", for postfix="+postfix.toString
        //+" and sizeAndInstances="+sizeAndInstances._2.mkString("[",",","]"))
        if ( counts.contains(x) ){
          // TODO should mapA++mapB is enough ?? or => instances=instancesA++instancesB.map{case(k,v)=>k->(v++instancesA.getOrElse(k ,Iterable.Empty))}
          if (counts(x).contains(postfix.sequenceId)){
            counts(x).apply(postfix.sequenceId)++=sizeAndInstances._2
          }else{
            counts(x).put(postfix.sequenceId, sizeAndInstances._2)
          }
        }else{
          counts.put(x, mutable.Map(postfix.sequenceId->sizeAndInstances._2))
        }
      }
    }
    val freqItems = counts.toSeq.filter { case (_, instances) =>
      instances.keys.size >= minCount
    }.sortBy{case(item, instances)=>(item, instances.keys.size)}
    // project and recursively call genFreqPatterns
    freqItems.toIterator.flatMap { case (item, instances) =>
      val newPrefix = prefix :+ (item, instances, maxDTofPatterns)
      //log.warn("d before recursive on newPrefix ="+newPrefix)
      Iterator.single((newPrefix, newPrefix.instances.keys.size.toLong)) ++ {
        //log.warn("d recursive on newPrefix ="+newPrefix)
        //log.warn("d recursive on newPrefix instances ="+newPrefix.instances.mkString("[",",","]"))
        //TODO costly????
        val projected = postfixes.flatMap{ _.projectFull(new Prefix(Array(0, item), 1, mutable.Map.empty)) }
          .filter(_.nonEmpty)
        //log.warn("d recursive on projected ="+projected.mkString("[",",","]"))
        genFreqPatterns(newPrefix, projected, maxDTofPatterns)
      }
    }
  }
}

private object MyLocalPrefixSpan {

  /**
    * Represents a prefix stored as a list in reversed order.
    * @param items items in the prefix in reversed order
    * @param length length of the prefix, not counting delimiters
    */
  class ReversedPrefix private (val items: List[Int], val length: Int, val instances: mutable.Map[Long, Set[Long]])
    extends Serializable {

    def merge(newInstances: mutable.Map[Long, Set[Long]], currLength:Int, newLength:Int, maxDTofPatterns: Int)
    :mutable.Map[Long, Set[Long]] = {
      //var log = LogManager.getRootLogger
      //log.warn("d merging:currentInstances="+instances.mkString("[",",","]")+" and newInstances="+newInstances.mkString("[",",","]"))
      var result:mutable.Map[Long, Set[Long]] = mutable.Map.empty
      if (instances.nonEmpty){
        newInstances.keys.foreach(newKey=>{
          if (instances.contains(newKey)){ //expected to be always true
            var newTimes = newInstances(newKey).toBuffer.sorted
            var newSet: Set[Long]=Set.empty
            while (newTimes.nonEmpty){
              var newTimesArray = newTimes.take(newLength)
              var newTimesFirst = newTimesArray.head
              var currTimes = instances(newKey).toBuffer.sorted
              while(currTimes.nonEmpty && newTimes.nonEmpty) {
                var currTimesArray = currTimes.take(currLength)
                var bestTimes = currTimesArray.filter(prevTime => newTimesFirst - prevTime <= maxDTofPatterns && newTimesFirst - prevTime > 0)
                if (bestTimes.size > 0) { //found candidates
                  var best = bestTimes.max
                  if (!newSet.contains(best)) {
                    newSet = newSet ++ currTimesArray.toSet ++ newTimesArray.toSet //add nearest
                  }
                  newTimes = newTimes -- newTimesArray //remove found times
                  if (newTimes.nonEmpty){
                    newTimesArray = newTimes.take(newLength)
                    newTimesFirst = newTimesArray.head
                  }
                  currTimes = currTimes -- currTimesArray
                } else {
                  currTimes = currTimes -- currTimesArray
                }
              }
              newTimes = newTimes -- newTimesArray //remove checked times
            }//next newTimes batch
            //if (newSet.nonEmpty)
              result.put(newKey,newSet)
          }
        })
      }else{
        result = newInstances
      }
      //log.warn("d result="+result.mkString("[",",","]"))
      result
    }

    /**
      * Expands the prefix by one item.
      */
    def :+(item: Int, newInstances: mutable.Map[Long, Set[Long]], maxDTofPatterns: Int): ReversedPrefix = {
      require(item != 0)
      //var log = LogManager.getRootLogger
      //log.warn("d prefix="+items.mkString("[",",","]")+", new item="+item )
      var mergedInstances = merge( newInstances, length, 1, maxDTofPatterns)
      //log.warn("d mergedInstances="+mergedInstances.mkString("[",",","]") )
      if (item < 0) {
        new ReversedPrefix(-item :: items, length + 1, mergedInstances)
      } else {
        new ReversedPrefix(item :: 0 :: items, length + 1, mergedInstances)
      }
    }

    override def toString: String = {
      "ReversedPrefix: items="+items.mkString("[",",","]")+", instances="+instances.mkString("[",",","]")
    }

    /**
      * Converts this prefix to a sequence.
      */
    def toSequence: Array[Int] = (0 :: items).toArray.reverse
  }

  object ReversedPrefix {
    /** An empty prefix. */
    val empty: ReversedPrefix = new ReversedPrefix(List.empty, 0, mutable.Map.empty)
  }
}
