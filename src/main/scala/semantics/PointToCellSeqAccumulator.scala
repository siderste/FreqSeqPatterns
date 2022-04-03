package semantics

import org.apache.spark.util.AccumulatorV2

class PointToCellSeqAccumulator(initialValue: Seq[(Long, Long, String, String)], name: String)
  extends AccumulatorV2[(Long, Long, String, String), Seq[(Long, Long, String, String)]]{

  //tuples are of form (tid, time, cell), OUT is Seq((tid, time, cell))
  private var _seq: Seq[(Long, Long, String, String)] = initialValue
  private var _name: String = name

  def this()={
    this(Seq.empty, "")
  }

  override def isZero: Boolean = _seq.isEmpty

  override def copy(): AccumulatorV2[(Long, Long, String, String), Seq[(Long, Long, String, String)]] = {
    val newSeq = new PointToCellSeqAccumulator(Seq.empty, "")
    newSeq._seq = this._seq
    newSeq._name = this._name
    newSeq
  }

  override def reset(): Unit = {
    this._seq = Seq.empty
    this._name = ""
  }

  override def add(v: (Long, Long, String, String)): Unit = {
    _seq = _seq++Seq(v)
  }

  override def merge(other: AccumulatorV2[(Long, Long, String, String), Seq[(Long, Long, String, String)]]): Unit = {
    _seq = _seq++other.value
  }

  override def value: Seq[(Long, Long, String, String)] = _seq
}
