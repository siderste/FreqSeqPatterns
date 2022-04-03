package semantics

import org.apache.spark.util.AccumulatorV2

import scala.collection.concurrent.TrieMap

class StingToGivenIdMapAccumulator(initialValue: TrieMap[String, Int], name: String)
  extends AccumulatorV2[(String, Int), TrieMap[String, Int]]{

  //IN is (string, id) OUT is string->(id)
  private var _map: TrieMap[String, Int] = initialValue
  private var _name: String = name

  def this()={
    this(TrieMap().empty, "")
  }

  override def isZero: Boolean = _map.isEmpty

  override def copy(): AccumulatorV2[(String, Int), TrieMap[String, Int]] = {
    val newMap = new StingToGivenIdMapAccumulator(TrieMap().empty, "")
    newMap._map = this._map
    newMap._name = this._name
    newMap
  }

  override def reset(): Unit = {
    this._map = TrieMap().empty
    this._name = ""
  }

  override def add(v: (String, Int)): Unit = {
    if ( !_map.contains(v._1) ){
      _map.put(v._1, v._2)
    }
  }

  override def merge(other: AccumulatorV2[(String, Int), TrieMap[String, Int]]): Unit = {
    for ((inString, id) <- other.value) {
      this.add(inString, id)
    }
  }

  override def value: TrieMap[String, Int] = _map
}
