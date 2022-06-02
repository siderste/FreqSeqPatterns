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

import org.apache.spark.internal.Logging

import scala.collection.mutable

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
    //to iterative from recursion
    if (1 == 2) {
      log.warn("inside iterative")
      var result: Iterator[(ReversedPrefix, Long)] = Iterator.empty
      //spark.sparkContext.register(result, "result")
      var currPrefix: ReversedPrefix = prefix
      var currPostFixes: Array[Postfix] = postfixes
      val counts = findFrequentItems(currPostFixes)
      val freqItems = counts.toSeq.filter { case (_, trajAndInstances) => trajAndInstances.keys.size >= minCount
        }.sortBy{case(item, trajAndInstances)=>(item, trajAndInstances.keys.size)}
      while( !(maxPatternLength == currPrefix.length || currPostFixes.length < minCount) ){
        log.warn("currPostFixes.length="+currPostFixes.length)
        val counts = findFrequentItems(currPostFixes)
        log.warn("counts.keys.size="+counts.keys.size)
        val freqItems = counts.toSeq.filter { case (_, trajAndInstances) => trajAndInstances.keys.size >= minCount
          }.sortBy{case(item, trajAndInstances)=>(item, trajAndInstances.keys.size)}
        log.warn("freqItems.length="+freqItems.length)
        freqItems.toIterator.flatMap { case (item, instances) =>
          val newPrefix = currPrefix :+ (item, instances, maxDTofPatterns)
          log.warn("newPrefix.items.length="+newPrefix.items.length)
          currPrefix = newPrefix
          currPostFixes = postfixes.flatMap{ _.projectFull(new Prefix(Array(0, item), 1, mutable.Map.empty)) }.filter(_.nonEmpty)
          log.warn("in flat currPostFixes.length="+currPostFixes.length)
          result ++ Iterator.single((newPrefix, newPrefix.instances.keys.size.toLong))
        }
      }
      result ++ Iterator.empty // although not needed as it is empty
    }else{
      log.warn("inside recursive")
      log.warn("before counts postfixes.length="+postfixes.length)
      if (maxPatternLength == prefix.length || postfixes.length < minCount) {return Iterator.empty }
      val counts = findFrequentItems(postfixes)
      val freqItems = counts.toSeq.filter { case (_, trajAndInstances) => trajAndInstances.keys.size >= minCount
        }.sortBy{case(item, trajAndInstances)=>(item, trajAndInstances.keys.size)}
      log.warn("before flat freqItems.length="+freqItems.length)
      freqItems.toIterator.flatMap { case (item, trajAndInstances) =>
        val newPrefix = prefix :+ (item, trajAndInstances, maxDTofPatterns)
        Iterator.single((newPrefix, newPrefix.instances.keys.size.toLong)) ++ {
          log.warn("before projected for item="+item)
          val projected = postfixes.flatMap{ _.projectFull(new Prefix(Array(0, item), 1, trajAndInstances)) }.filter(_.nonEmpty)
          log.warn("before recurs in flat projected.length="+projected.length)
          genFreqPatterns(newPrefix, projected, maxDTofPatterns)
        }
      }
    } // if 1==2
  }

  // find frequent items
  private def findFrequentItems(postfixes: Array[Postfix]): mutable.Map[Int, mutable.Map[Long, mutable.SortedSet[Item]]] ={
    val counts = mutable.Map.empty[Int, mutable.Map[Long, mutable.SortedSet[Item]]] // counts(prefix->(seqid->[items]))
    postfixes.foreach { postfix =>
      postfix.genPrefixItems.foreach { case (x, sizeAndInstances) =>
        if ( counts.contains(x) ){
          //println("sizeAndInstances=" + sizeAndInstances._2.mkString("[", ",", "]"))
          // TODO should mapA++mapB is enough ?? or => instances=instancesA++instancesB.map{case(k,v)=>k->(v++instancesA.getOrElse(k ,Iterable.Empty))}
          if (counts(x).contains(postfix.sequenceId)){
            if (postfix.sequenceId==5993630) {
              //println("counts instances=" + counts(x).apply(postfix.sequenceId).mkString("[", ",", "]"))
              //println("sizeAndInstances._2 instances=" + sizeAndInstances._2.mkString("[", ",", "]"))
            }
            counts(x).apply(postfix.sequenceId)++=sizeAndInstances._2
            //println("counts after instances="+counts(x).apply(postfix.sequenceId).mkString("[",",","]"))
          }else{
            counts(x).put(postfix.sequenceId, sizeAndInstances._2)
          }
        }else{
          counts.put(x, mutable.Map(postfix.sequenceId->sizeAndInstances._2))
        }
      }
    }
    counts
  }



}

private object MyLocalPrefixSpan {

  /**
    * Represents a prefix stored as a list in reversed order.
    * @param items items in the prefix in reversed order
    * @param length length of the prefix, not counting delimiters
    */
  class ReversedPrefix private (val items: List[Int], val length: Int, val instances: mutable.Map[Long, mutable.SortedSet[Item]])
    extends Serializable {

    def merge(newInstances: mutable.Map[Long, mutable.SortedSet[Item]], currLength:Int, newLength:Int, maxDTofPatterns: Int)
    :mutable.Map[Long, mutable.SortedSet[Item]] = {
      //var log = LogManager.getRootLogger
      //log.warn("d merging:currentInstances="+instances.mkString("[",",","]")+" and newInstances="+newInstances.mkString("[",",","]"))
      var result:mutable.Map[Long, mutable.SortedSet[Item]] = mutable.Map.empty
      if (instances.nonEmpty){
        newInstances.keys.foreach(newTraj=>{
          if (instances.contains(newTraj)){ //expected to be always true
            var newInstancesTimes = newInstances(newTraj).toBuffer.sorted
            var newInstancesSet: mutable.SortedSet[Item]=mutable.SortedSet.empty
            while (newInstancesTimes.nonEmpty){
              var newInstancesTimesArray = newInstancesTimes.take(newLength)
              var newInstancesTimesFirst = newInstancesTimesArray.head
              var currInstancesTimes = instances(newTraj).toBuffer.sorted
              while(currInstancesTimes.nonEmpty && newInstancesTimes.nonEmpty) {
                var currInstancesTimesArray = currInstancesTimes.take(currLength)
                var bestTimes = currInstancesTimesArray
                  .filter(currInstanceTime => newInstancesTimesFirst.time - currInstanceTime.time <= maxDTofPatterns
                    && newInstancesTimesFirst.time - currInstanceTime.time > 0)
                if (bestTimes.size > 0) { //found candidates
                  var best = bestTimes.max
                  if (!newInstancesSet.contains(best)) {
                    newInstancesSet = newInstancesSet ++ currInstancesTimesArray.toSet ++ newInstancesTimesArray.toSet //add nearest
                  }
                  newInstancesTimes = newInstancesTimes -- newInstancesTimesArray //remove found times
                  if (newInstancesTimes.nonEmpty){
                    newInstancesTimesArray = newInstancesTimes.take(newLength)
                    newInstancesTimesFirst = newInstancesTimesArray.head
                  }
                  currInstancesTimes = currInstancesTimes -- currInstancesTimesArray
                } else {
                  currInstancesTimes = currInstancesTimes -- currInstancesTimesArray
                }
              }
              newInstancesTimes = newInstancesTimes -- newInstancesTimesArray //remove checked times
            }//next newInstancesTimes batch
            //if (newInstancesSet.nonEmpty)
              result.put(newTraj,newInstancesSet)
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
    def :+(item: Int, newInstances: mutable.Map[Long, mutable.SortedSet[Item]], maxDTofPatterns: Int): ReversedPrefix = {
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
