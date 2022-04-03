package semantics

import org.apache.spark.util.AccumulatorV2
import scala.collection.concurrent.TrieMap

class StringToTotalCountsMapAccumulator(initialValue: TrieMap[String, Int], name: String)
  extends AccumulatorV2[(String, Int), TrieMap[String, Int]]{

  //IN is (string, numOfPoints), OUT is string->totalNumOfPoints
  private var _countsMap: TrieMap[String, Int] = initialValue
  private var _name: String = name

  def this()={
    this(TrieMap().empty, "")
  }

  override def isZero: Boolean = _countsMap.isEmpty

  override def copy(): AccumulatorV2[(String, Int), TrieMap[String, Int]] = {
    val newMap = new StringToTotalCountsMapAccumulator(TrieMap().empty, "")
    newMap._countsMap = this._countsMap
    newMap._name = this._name
    newMap
  }

  override def reset(): Unit = {
    this._countsMap = TrieMap().empty
    this._name = ""
  }

  override def add(v: (String, Int)): Unit = {
    if ( !_countsMap.contains(v._1) ){
      _countsMap.put(v._1, v._2)
    }else{
      _countsMap.put(v._1, v._2+_countsMap.apply(v._1))
    }
  }

  override def merge(other: AccumulatorV2[(String, Int), TrieMap[String, Int]]): Unit = {
    for ((inString, numOfPoints) <- other.value) {
      this.add(inString, numOfPoints)
    }
  }

  override def value: TrieMap[String, Int] = _countsMap
}
