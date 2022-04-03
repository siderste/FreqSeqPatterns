package preprocess

import org.apache.spark.util.AccumulatorV2

import scala.collection.concurrent.TrieMap

class NomCombCntAccu(initialValue: TrieMap[String, Long], name: String)
  extends AccumulatorV2[(String, Long), TrieMap[String, Long]]{

  private var _wordsToCnt: TrieMap[String, Long] = initialValue
  //keys are of form: "words" -> Long
  private var _name: String = name

  def this()={
    this(TrieMap().empty, "")
  }

  override def isZero: Boolean = _wordsToCnt.isEmpty

  override def copy(): AccumulatorV2[(String, Long), TrieMap[String, Long]] = {
    val newMap = new NomCombCntAccu(TrieMap().empty, "")
    newMap._wordsToCnt = this._wordsToCnt
    newMap._name = this._name
    newMap
  }

  override def reset(): Unit = {
    this._wordsToCnt = TrieMap().empty
    this._name = ""
  }

  override def add(v: (String, Long)): Unit = {
    if ( _wordsToCnt.contains(v._1) ){
      _wordsToCnt.put(v._1, _wordsToCnt.apply(v._1) + v._2)
    }else{
      _wordsToCnt.put(v._1, v._2)
    }
  }

  override def merge(other: AccumulatorV2[(String, Long), TrieMap[String, Long]]): Unit = {
    //for each key of the other, if exists in this then compare it, if not add it
    for ((k,v) <- other.value) {
      this.add(k,v)
    }
  }

  override def value: TrieMap[String, Long] = _wordsToCnt
}

