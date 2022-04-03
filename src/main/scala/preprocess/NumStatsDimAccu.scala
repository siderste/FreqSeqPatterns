package preprocess

import org.apache.spark.util.AccumulatorV2

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

class NumStatsDimAccu(initialValue: TrieMap[String, Double], name: String)
  extends AccumulatorV2[(String, Double), TrieMap[String, Double]]{

  private var _dimToStats: TrieMap[String, Double] = initialValue
  private var _dims: ArrayBuffer[String] = ArrayBuffer()
  //keys are of form: "min_d_x","max_d_x","sum_d_x","su2_d_x","cnt_d_x","mea_d_x","var_d_x" -> double
  private var _name: String = name

  def this()={
    this(TrieMap().empty, "")
  }

  override def isZero: Boolean = _dimToStats.isEmpty

  override def copy(): AccumulatorV2[(String, Double), TrieMap[String, Double]] = {
    val newMap = new NumStatsDimAccu(TrieMap().empty, "")
    newMap._dimToStats = this._dimToStats
    newMap._name = this._name
    newMap
  }

  override def reset(): Unit = {
    this._dimToStats = TrieMap().empty
    this._name = ""
  }

  //keys are of form: "min_d_x","max_d_x","sum_d_x","su2_d_x","cnt_d_x","mea_d_x","var_d_x" -> double
  //input is of ("d_x", double)
  override def add(v: (String, Double)): Unit = {
    if ( ! _dims.contains(v._1) )
      _dims += v._1
    if (_dimToStats.contains("min_"+v._1)){
      //then stats are set
      if ( v._2 < _dimToStats.apply("min_"+v._1) )
        _dimToStats.put("min_"+v._1, v._2)
      if ( v._2 > _dimToStats.apply("max_"+v._1) )
        _dimToStats.put("max_"+v._1, v._2)
      _dimToStats.put("sum_"+v._1, _dimToStats.apply("sum_"+v._1) + v._2)
      _dimToStats.put("su2_"+v._1, _dimToStats.apply("su2_"+v._1) + v._2*v._2)
      _dimToStats.put("cnt_"+v._1, _dimToStats.apply("cnt_"+v._1) + 1L)
    }else{
      //set stats
      _dimToStats.put("min_"+v._1, v._2)
      _dimToStats.put("max_"+v._1, v._2)
      _dimToStats.put("sum_"+v._1, v._2)
      _dimToStats.put("su2_"+v._1, v._2*v._2)
      _dimToStats.put("cnt_"+v._1, 1L)
    }
  }

  override def merge(other: AccumulatorV2[(String, Double), TrieMap[String, Double]]): Unit = {
    //for each key of the other,
    //keys are of form: "min_d_x","max_d_x","sum_d_x","su2_d_x","cnt_d_x","mea_d_x","var_d_x" -> double
    for ((k,v) <- other.value) {
      if ( _dimToStats.contains(k) ){
        if (k.startsWith("min_")){
          if ( _dimToStats.apply(k) > v )
            _dimToStats.put(k, v)
        }
        if (k.startsWith("max_")){
          if ( _dimToStats.apply(k) < v )
            _dimToStats.put(k, v)
        }
        if (k.startsWith("sum_")){
          _dimToStats.put(k, _dimToStats.apply(k) + v)
        }
        if (k.startsWith("su2_")){
          _dimToStats.put(k, _dimToStats.apply(k) + v)
        }
        if (k.startsWith("cnt_")){
          _dimToStats.put(k, _dimToStats.apply(k) + v)
        }
      }else{
        _dimToStats.put(k, v)
      }
    }
  }

  override def value: TrieMap[String, Double] = {
    // TODO for each dim add mean and variance
    //keys are of form: "min_d_x","max_d_x","sum_d_x","su2_d_x","cnt_d_x","mea_d_x","var_d_x" -> double
    _dims.foreach(dim=>{
      var mean = _dimToStats.apply("sum_"+dim)/_dimToStats.apply("cnt_"+dim)
      _dimToStats.put("mea_"+dim, mean)
      var mean2 = _dimToStats.apply("su2_"+dim)/_dimToStats.apply("cnt_"+dim)
      var variance = Math.sqrt(mean2 - mean*mean)
      _dimToStats.put("var_"+dim, variance)
    })
    _dimToStats
  }
}

